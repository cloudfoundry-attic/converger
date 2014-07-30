package main_test

import (
	"fmt"
	"syscall"
	"time"

	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	"github.com/cloudfoundry/storeadapter/test_helpers"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/lager/lagertest"

	"github.com/cloudfoundry-incubator/converger/converger_runner"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("Main", func() {
	var (
		etcdRunner *etcdstorerunner.ETCDClusterRunner
		bbs        *Bbs.BBS
		runner     *converger_runner.ConvergerRunner

		fileServerPresence services_bbs.Presence

		taskKickInterval = 1 * time.Second
	)

	BeforeSuite(func() {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test"))

		convergerBinPath, err := gexec.Build("github.com/cloudfoundry-incubator/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())

		runner = converger_runner.New(convergerBinPath, etcdCluster, "info")
	})

	AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()

		runner.Start(1*time.Second, taskKickInterval, 30*time.Minute, 30*time.Second, 300*time.Second)
		Consistently(runner.Session.ExitCode).Should(Equal(-1))

		var err error
		var presenceStatus <-chan bool

		fileServerPresence, presenceStatus, err = bbs.MaintainFileServerPresence(time.Second, "http://some.file.server", "file-server-id")
		Ω(err).ShouldNot(HaveOccurred())

		Eventually(presenceStatus).Should(Receive(BeTrue()))
		test_helpers.NewStatusReporter(presenceStatus)
	})

	AfterEach(func() {
		fileServerPresence.Remove()
		runner.KillWithFire()
		etcdRunner.Stop()
	})

	Context("when a claimed task with a dead executor is present", func() {
		JustBeforeEach(func() {
			task := models.Task{
				Domain: "tests",

				Guid:  "task-guid",
				Stack: "stack",
				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Path: "cat",
							Args: []string{"/tmp/file"},
						},
					},
				},
			}

			err := bbs.DesireTask(task)
			Ω(err).ShouldNot(HaveOccurred())

			err = bbs.ClaimTask(task.Guid, "dead-executor")
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("eventually marks the task as failed", func() {
			Eventually(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(HaveLen(1))
			tasks, err := bbs.GetAllTasks()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(tasks).Should(HaveLen(1))
			Ω(tasks[0].State).Should(Equal(models.TaskStateCompleted))
			Ω(tasks[0].Failed).Should(BeTrue())
		})
	})

	Describe("when an LRP is desired", func() {
		JustBeforeEach(func() {
			err := bbs.DesireLRP(models.DesiredLRP{
				Domain: "tests",

				ProcessGuid: "the-guid",

				Stack: "some-stack",

				Instances: 3,
				MemoryMB:  128,
				DiskMB:    512,

				Actions: []models.ExecutorAction{
					{
						Action: models.RunAction{
							Path: "the-start-command",
						},
					},
				},
			})
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("registers an app desire in etcd", func() {
			Eventually(bbs.GetAllDesiredLRPs).Should(HaveLen(1))
		})

		Context("for an app that is not running at all", func() {
			It("desires N start auctions in the BBS", func() {
				Eventually(bbs.GetAllLRPStartAuctions, 0.5).Should(HaveLen(3))
			})
		})

		Context("for an app that is missing instances", func() {
			BeforeEach(func() {
				bbs.ReportActualLRPAsRunning(models.ActualLRP{
					ProcessGuid:  "the-guid",
					InstanceGuid: "a",
					Index:        0,
				}, "executor-id")
			})

			It("start auctions for the missing instances", func() {
				Eventually(bbs.GetAllLRPStartAuctions, 0.5).Should(HaveLen(2))
				auctions, err := bbs.GetAllLRPStartAuctions()
				Ω(err).ShouldNot(HaveOccurred())

				indices := []int{auctions[0].Index, auctions[1].Index}
				Ω(indices).Should(ContainElement(1))
				Ω(indices).Should(ContainElement(2))

				Consistently(bbs.GetAllLRPStartAuctions).Should(HaveLen(2))
			})
		})

		Context("for an app that has extra instances", func() {
			BeforeEach(func() {
				bbs.ReportActualLRPAsRunning(models.ActualLRP{
					ProcessGuid:  "the-guid",
					InstanceGuid: "a",
					Index:        0,
				}, "executor-id")

				bbs.ReportActualLRPAsRunning(models.ActualLRP{
					ProcessGuid:  "the-guid",
					InstanceGuid: "b",
					Index:        1,
				}, "executor-id")

				bbs.ReportActualLRPAsRunning(models.ActualLRP{
					ProcessGuid:  "the-guid",
					InstanceGuid: "c",
					Index:        2,
				}, "executor-id")

				bbs.ReportActualLRPAsRunning(models.ActualLRP{
					ProcessGuid:  "the-guid",
					InstanceGuid: "d-extra",
					Index:        3,
				}, "executor-id")
			})

			It("stops the extra instances", func() {
				Consistently(bbs.GetAllLRPStartAuctions, 0.5).Should(BeEmpty())
				Eventually(bbs.GetAllStopLRPInstances).Should(HaveLen(1))
				stopInstances, err := bbs.GetAllStopLRPInstances()
				Ω(err).ShouldNot(HaveOccurred())

				Ω(stopInstances[0].ProcessGuid).Should(Equal("the-guid"))
				Ω(stopInstances[0].Index).Should(Equal(3))
				Ω(stopInstances[0].InstanceGuid).Should(Equal("d-extra"))
			})
		})

		Context("when an app is no longer desired", func() {
			JustBeforeEach(func() {
				Eventually(bbs.GetAllDesiredLRPs).Should(HaveLen(1))
				err := bbs.RemoveDesiredLRPByProcessGuid("the-guid")
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("should remove the desired state from etcd", func() {
				Eventually(bbs.GetAllDesiredLRPs).Should(HaveLen(0))
			})
		})
	})

	Describe("then receives the SIGINT signal", func() {
		It("exits successfully", func() {
			runner.Session.Command.Process.Signal(syscall.SIGINT)
			Eventually(runner.Session, 4).Should(gexec.Exit(0))
		})
	})

	Describe("then receives the SIGTERM signal", func() {
		It("exits successfully", func() {
			runner.Session.Command.Process.Signal(syscall.SIGTERM)
			Eventually(runner.Session, 4).Should(gexec.Exit(0))
		})
	})
})
