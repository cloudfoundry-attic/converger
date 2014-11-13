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

		etcdClient.Create(storeadapter.StoreNode{
			Key:   shared.CellSchemaPath(cellPresence.CellID),
			Value: cellPresence.ToJSON(),
		})
	})

	AfterEach(func() {
		runner.KillWithFire()
		etcdRunner.Stop()
	})

	startConverger := func() {
		runner.Start(convergeRepeatInterval, taskKickInterval, 30*time.Minute, expireCompletedTaskDuration, 30*time.Second, 300*time.Second)
		time.Sleep(convergeRepeatInterval)
	}

	createClaimedTaskWithDeadCell := func() {
		task := models.Task{
			Domain: "tests",

			TaskGuid: "task-guid",
			Stack:    "stack",
			Action: models.ExecutorAction{
				Action: models.RunAction{
					Path: "cat",
					Args: []string{"/tmp/file"},
				},
			},
		}

		err := bbs.DesireTask(task)
		Ω(err).ShouldNot(HaveOccurred())

		err = bbs.ClaimTask(task.TaskGuid, "dead-cell")
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

			Action: models.ExecutorAction{
				Action: models.RunAction{
					Path: "the-start-command",
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

		Context("when a claimed task with a dead cell is present", func() {
			JustBeforeEach(createClaimedTaskWithDeadCell)

			It("does not change the task", func() {
				Consistently(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(BeEmpty())
			})
		})
	}

	Context("when the converger has the lock", func() {
		JustBeforeEach(startConverger)

		Describe("when an LRP is desired", func() {
			BeforeEach(desireLRP)

			Context("for an app that is not running at all", func() {
				It("desires N start auctions in the BBS", func() {
					Eventually(bbs.GetAllLRPStartAuctions, 0.5).Should(HaveLen(3))
				})
			})

			Context("for an app that is missing instances", func() {
				BeforeEach(func() {
					err := bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "a",
						Domain:       "the-domain",
						Index:        0,
					}, "the-cell-id")
					Ω(err).ShouldNot(HaveOccurred())
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
					err := bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "a",
						Domain:       "the-domain",
						Index:        0,
					}, "the-cell-id")
					Ω(err).ShouldNot(HaveOccurred())

					err = bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "b",
						Domain:       "the-domain",
						Index:        1,
					}, "the-cell-id")
					Ω(err).ShouldNot(HaveOccurred())

					err = bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "c",
						Domain:       "the-domain",
						Index:        2,
					}, "the-cell-id")
					Ω(err).ShouldNot(HaveOccurred())

					err = bbs.ReportActualLRPAsRunning(models.ActualLRP{
						ProcessGuid:  "the-guid",
						InstanceGuid: "d-extra",
						Domain:       "the-domain",
						Index:        3,
					}, "the-cell-id")
					Ω(err).ShouldNot(HaveOccurred())
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

		Describe("when a claimed task with a dead cell is present", func() {
			JustBeforeEach(createClaimedTaskWithDeadCell)

			It("marks the task as failed after the kick interval", func() {
				Eventually(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(HaveLen(1))
				tasks, err := bbs.GetAllTasks()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(tasks).Should(HaveLen(1))
				Ω(tasks[0].State).Should(Equal(models.TaskStateCompleted))
				Ω(tasks[0].Failed).Should(BeTrue())
			})

			It("deletes the task after the 'expire completed task' interval", func() {
				Eventually(bbs.GetAllCompletedTasks, taskKickInterval*2).Should(HaveLen(1))
				tasks, err := bbs.GetAllTasks()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(tasks).Should(HaveLen(1))
				Ω(tasks[0].State).Should(Equal(models.TaskStateCompleted))
				Ω(tasks[0].Failed).Should(BeTrue())

				guid := tasks[0].TaskGuid
				_, err = bbs.GetTaskByGuid(guid)
				Ω(err).ShouldNot(HaveOccurred())

				getTaskError := func() error {
					_, err := bbs.GetTaskByGuid(guid)
					return err
				}

				Consistently(getTaskError, expireCompletedTaskDuration-time.Second).ShouldNot(HaveOccurred())
				Eventually(getTaskError, expireCompletedTaskDuration+time.Second).Should(Equal(storeadapter.ErrorKeyNotFound))
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

			Describe("when a claimed task with a dead cell is present", func() {
				JustBeforeEach(createClaimedTaskWithDeadCell)

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
