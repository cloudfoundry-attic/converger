package main_test

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/converger/converger_runner"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("Main", func() {
	var (
		etcdRunner *etcdstorerunner.ETCDClusterRunner
		bbs        *Bbs.BBS
		runner     *converger_runner.ConvergerRunner

		fileServerPresence ifrit.Process

		taskKickInterval = 1 * time.Second

		etcdClient storeadapter.StoreAdapter

		convergeRepeatInterval = time.Second
	)

	BeforeSuite(func() {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

		etcdClient = etcdRunner.Adapter()
		bbs = Bbs.NewBBS(etcdClient, timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test"))

		convergerBinPath, err := gexec.Build("github.com/cloudfoundry-incubator/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())

		runner = converger_runner.New(convergerBinPath, etcdCluster, "info")
	})

	AfterSuite(func() {
		etcdRunner.Stop()
		gexec.CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()

		fileServerPresence = ifrit.Envoke(bbs.NewFileServerHeartbeat("http://some.file.server", "file-server-id", time.Second))

		executorPresence := models.ExecutorPresence{
			ExecutorID: "the-executor-id",
			Stack:      "the-stack",
		}

		etcdClient.Create(storeadapter.StoreNode{
			Key:   shared.ExecutorSchemaPath(executorPresence.ExecutorID),
			Value: executorPresence.ToJSON(),
		})
	})

	AfterEach(func() {
		fileServerPresence.Signal(os.Interrupt)
		Eventually(fileServerPresence.Wait()).Should(Receive(BeNil()))

		runner.KillWithFire()
		etcdRunner.Stop()
	})

	startConverger := func() {
		runner.Start(convergeRepeatInterval, taskKickInterval, 30*time.Minute, 30*time.Second, 300*time.Second)
		time.Sleep(convergeRepeatInterval)
	}

	createClaimedTaskWithDeadExecutor := func() {
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
	}

	desireLRP := func() {
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
	}

	itIsInactive := func() {
		Describe("when an LRP is desired", func() {
			JustBeforeEach(desireLRP)

			It("does not create start auctions for apps that are missing instances", func() {
				Consistently(bbs.GetAllLRPStartAuctions, 0.5).Should(BeEmpty())
			})
		})

		Context("when a claimed task with a dead executor is present", func() {
			JustBeforeEach(createClaimedTaskWithDeadExecutor)

			It("does not change the task", func() {
				Consistently(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(BeEmpty())
			})
		})
	}

	Context("when the converger has the lock", func() {
		BeforeEach(startConverger)

		Describe("when an LRP is desired", func() {
			JustBeforeEach(desireLRP)

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
					}, "the-executor-id")
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
					}, "the-executor-id")

					bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "b",
						Index:        1,
					}, "the-executor-id")

					bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "c",
						Index:        2,
					}, "the-executor-id")

					bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "d-extra",
						Index:        3,
					}, "the-executor-id")
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
		})

		Describe("when a claimed task with a dead executor is present", func() {
			JustBeforeEach(createClaimedTaskWithDeadExecutor)

			It("eventually marks the task as failed", func() {
				Eventually(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(HaveLen(1))
				tasks, err := bbs.GetAllTasks()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(tasks).Should(HaveLen(1))
				Ω(tasks[0].State).Should(Equal(models.TaskStateCompleted))
				Ω(tasks[0].Failed).Should(BeTrue())
			})
		})
	})

	Context("when the converger loses the lock", func() {
		BeforeEach(func() {
			startConverger()
			err := etcdClient.Update(storeadapter.StoreNode{
				Key:   shared.LockSchemaPath("converge_lock"),
				Value: []byte("something-else"),
			})
			Ω(err).ShouldNot(HaveOccurred())

			time.Sleep(convergeRepeatInterval + 10*time.Millisecond)
		})

		itIsInactive()

		It("exits with an error", func() {
			Eventually(runner.Session.ExitCode).Should(Equal(1))
		})
	})

	Context("when the converger initially does not have the lock", func() {
		BeforeEach(func() {
			err := etcdClient.Create(storeadapter.StoreNode{
				Key:   shared.LockSchemaPath("converge_lock"),
				Value: []byte("something-else"),
			})
			Ω(err).ShouldNot(HaveOccurred())

			startConverger()
		})

		itIsInactive()

		Describe("when the lock becomes available", func() {
			BeforeEach(func() {
				err := etcdClient.Delete(shared.LockSchemaPath("converge_lock"))
				Ω(err).ShouldNot(HaveOccurred())

				time.Sleep(convergeRepeatInterval + 10*time.Millisecond)
			})

			Describe("when an LRP is desired", func() {
				JustBeforeEach(desireLRP)

				Context("for an app that is not running at all", func() {
					It("desires N start auctions in the BBS", func() {
						Eventually(bbs.GetAllLRPStartAuctions, 0.5).Should(HaveLen(3))
					})
				})
			})

			Describe("when a claimed task with a dead executor is present", func() {
				JustBeforeEach(createClaimedTaskWithDeadExecutor)

				It("eventually marks the task as failed", func() {
					Eventually(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(HaveLen(1))
				})
			})
		})
	})

	Describe("signal handling", func() {
		BeforeEach(func() {
			startConverger()
		})

		Describe("when it receives SIGINT", func() {
			It("exits successfully", func() {
				runner.Session.Command.Process.Signal(syscall.SIGINT)
				Eventually(runner.Session, 4).Should(gexec.Exit(0))
			})
		})

		Describe("when it receives SIGTERM", func() {
			It("exits successfully", func() {
				runner.Session.Command.Process.Signal(syscall.SIGTERM)
				Eventually(runner.Session, 4).Should(gexec.Exit(0))
			})
		})
	})
})
