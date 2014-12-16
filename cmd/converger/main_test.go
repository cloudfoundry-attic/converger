package main_test

import (
	"fmt"
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

	"github.com/cloudfoundry-incubator/converger/converger_runner"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("Converger", func() {
	var (
		etcdRunner *etcdstorerunner.ETCDClusterRunner
		bbs        *Bbs.BBS
		runner     *converger_runner.ConvergerRunner

		taskKickInterval = 1 * time.Second

		expireCompletedTaskDuration = 3 * time.Second

		etcdClient storeadapter.StoreAdapter

		convergeRepeatInterval = time.Second
	)

	SynchronizedBeforeSuite(func() []byte {
		convergerBinPath, err := gexec.Build("github.com/cloudfoundry-incubator/converger/cmd/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())
		return []byte(convergerBinPath)
	}, func(convergerBinPath []byte) {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

		etcdClient = etcdRunner.Adapter()
		bbs = Bbs.NewBBS(etcdClient, timeprovider.NewTimeProvider(), lagertest.NewTestLogger("test"))

		runner = converger_runner.New(string(convergerBinPath), etcdCluster, "info")
	})

	SynchronizedAfterSuite(func() {
		etcdRunner.Stop()
	}, func() {
		gexec.CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()

		cellPresence := models.CellPresence{
			CellID: "the-cell-id",
			Stack:  "the-stack",
		}

		value, err := models.ToJSON(cellPresence)
		Ω(err).ShouldNot(HaveOccurred())
		etcdClient.Create(storeadapter.StoreNode{
			Key:   shared.CellSchemaPath(cellPresence.CellID),
			Value: value,
		})
	})

	AfterEach(func() {
		runner.KillWithFire()
		etcdRunner.Stop()
	})

	startConverger := func() {
		runner.Start(convergeRepeatInterval, taskKickInterval, 30*time.Minute, expireCompletedTaskDuration)
		time.Sleep(convergeRepeatInterval)
	}

	createRunningTaskWithDeadCell := func() {
		task := models.Task{
			Domain: "tests",

			TaskGuid: "task-guid",
			Stack:    "stack",
			Action: &models.RunAction{
				Path: "cat",
				Args: []string{"/tmp/file"},
			},
		}

		err := bbs.DesireTask(task)
		Ω(err).ShouldNot(HaveOccurred())

		err = bbs.StartTask(task.TaskGuid, "dead-cell")
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

			Action: &models.RunAction{
				Path: "the-start-command",
			},
		})
		Ω(err).ShouldNot(HaveOccurred())
	}

	itIsInactive := func() {
		Describe("when an LRP is desired", func() {
			JustBeforeEach(desireLRP)

			It("does not create start auctions for apps that are missing instances", func() {
				Consistently(bbs.ActualLRPs, 0.5).Should(BeEmpty())
			})
		})
	}

	Context("when the converger has the lock", func() {
		Describe("when an LRP is desired", func() {
			BeforeEach(desireLRP)

			It("creates N actual LRPs in the BBS", func() {
				Consistently(bbs.ActualLRPs, 0.5).Should(BeEmpty())

				startConverger()

				Eventually(bbs.ActualLRPs, 0.5).Should(HaveLen(3))
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
		})

		It("exits with an error", func() {
			Eventually(runner.Session.ExitCode, 5*time.Second).Should(Equal(1))
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
				Context("for an app that is not running at all", func() {
					It("creates N actual LRPs in the BBS", func() {
						Consistently(bbs.ActualLRPs, 0.5).Should(BeEmpty())

						desireLRP()

						Eventually(bbs.ActualLRPs, 0.5).Should(HaveLen(3))
					})
				})
			})

			Describe("when a running task with a dead cell is present", func() {
				JustBeforeEach(createRunningTaskWithDeadCell)

				It("eventually marks the task as failed", func() {
					Eventually(bbs.CompletedTasks, taskKickInterval*2).Should(HaveLen(1))
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
