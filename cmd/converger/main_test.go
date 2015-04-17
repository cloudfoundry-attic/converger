package main_test

import (
	"fmt"
	"syscall"
	"time"

	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/converger/cmd/converger/testrunner"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("Converger", func() {
	const (
		exitDuration = 4 * time.Second
	)

	var (
		etcdRunner *etcdstorerunner.ETCDClusterRunner
		bbs        *Bbs.BBS
		runner     *testrunner.ConvergerRunner

		consulRunner  *consuladapter.ClusterRunner
		consulSession *consuladapter.Session

		convergeRepeatInterval      time.Duration
		taskKickInterval            time.Duration
		expireCompletedTaskDuration time.Duration

		etcdClient storeadapter.StoreAdapter

		logger lager.Logger
	)

	SynchronizedBeforeSuite(func() []byte {
		convergerBinPath, err := Build("github.com/cloudfoundry-incubator/converger/cmd/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())
		return []byte(convergerBinPath)
	}, func(convergerBinPath []byte) {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

		etcdClient = etcdRunner.Adapter()

		consulRunner = consuladapter.NewClusterRunner(
			9001+config.GinkgoConfig.ParallelNode*consuladapter.PortOffsetLength,
			1,
			"http",
		)

		logger = lagertest.NewTestLogger("test")

		runner = testrunner.New(string(convergerBinPath), etcdCluster, consulRunner.ConsulCluster(), "info")
	})

	SynchronizedAfterSuite(func() {
		etcdRunner.Stop()
	}, func() {
		CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()
		consulRunner.Start()
		consulRunner.WaitUntilReady()

		consulSession = consulRunner.NewSession("a-session")
		bbs = Bbs.NewBBS(etcdClient, consulSession, "http://receptor.bogus.com", clock.NewClock(), logger)

		capacity := models.NewCellCapacity(512, 1024, 124)
		cellPresence := models.NewCellPresence("the-cell-id", "1.2.3.4", "the-zone", capacity)

		value, err := models.ToJSON(cellPresence)
		Ω(err).ShouldNot(HaveOccurred())

		_, err = consulSession.SetPresence(shared.CellSchemaPath(cellPresence.CellID), value)
		Ω(err).ShouldNot(HaveOccurred())

		convergeRepeatInterval = 500 * time.Millisecond
		taskKickInterval = convergeRepeatInterval
		expireCompletedTaskDuration = 3 * convergeRepeatInterval
	})

	AfterEach(func() {
		runner.KillWithFire()
		consulRunner.Stop()
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
			RootFS:   "some:rootfs",
			Action: &models.RunAction{
				Path: "cat",
				Args: []string{"/tmp/file"},
			},
		}

		err := bbs.DesireTask(logger, task)
		Ω(err).ShouldNot(HaveOccurred())

		_, err = bbs.StartTask(logger, task.TaskGuid, "dead-cell")
		Ω(err).ShouldNot(HaveOccurred())
	}

	itIsInactive := func() {
		Describe("when a task is desired but its cell is dead", func() {
			JustBeforeEach(createRunningTaskWithDeadCell)

			It("does not converge the task", func() {
				Consistently(func() ([]models.Task, error) {
					return bbs.CompletedTasks(logger)
				}, 5*convergeRepeatInterval).Should(BeEmpty())
			})
		})
	}

	Context("when the converger has the lock", func() {
		Describe("when a task is desired but its cell is dead", func() {
			JustBeforeEach(createRunningTaskWithDeadCell)

			It("marks the task as completed and failed", func() {
				Consistently(func() ([]models.Task, error) {
					return bbs.CompletedTasks(logger)
				}, 0.5).Should(BeEmpty())

				startConverger()

				Eventually(func() ([]models.Task, error) {
					return bbs.CompletedTasks(logger)
				}, 5*convergeRepeatInterval).Should(HaveLen(1))
			})
		})
	})

	Context("when the converger loses the lock", func() {
		BeforeEach(func() {
			startConverger()
			Eventually(runner.Session, 5*time.Second).Should(gbytes.Say("acquire-lock-succeeded"))

			consulRunner.Reset()
		})

		It("exits with an error", func() {
			Eventually(runner.Session, exitDuration).Should(Exit(1))
		})
	})

	Context("when the converger initially does not have the lock", func() {
		var otherSession *consuladapter.Session

		BeforeEach(func() {
			otherSession = consulRunner.NewSession("other-session")
			err := otherSession.AcquireLock(shared.LockSchemaPath("converge_lock"), []byte("something-else"))
			Ω(err).ShouldNot(HaveOccurred())

			startConverger()
		})

		itIsInactive()

		Describe("when the lock becomes available", func() {
			BeforeEach(func() {
				otherSession.Destroy()
				time.Sleep(convergeRepeatInterval + 10*time.Millisecond)
			})

			Describe("when a running task with a dead cell is present", func() {
				JustBeforeEach(createRunningTaskWithDeadCell)

				It("eventually marks the task as failed", func() {
					Eventually(func() ([]models.Task, error) {
						return bbs.CompletedTasks(logger)
					}, 5*convergeRepeatInterval).Should(HaveLen(1))
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
				Eventually(runner.Session, exitDuration).Should(Exit(0))
			})
		})

		Describe("when it receives SIGTERM", func() {
			It("exits successfully", func() {
				runner.Session.Command.Process.Signal(syscall.SIGTERM)
				Eventually(runner.Session, exitDuration).Should(Exit(0))
			})
		})
	})

	Context("when etcd is down", func() {
		BeforeEach(func() {
			etcdRunner.Stop()
			startConverger()
		})

		AfterEach(func() {
			etcdRunner.Start()
		})

		It("starts", func() {
			Consistently(runner.Session).ShouldNot(Exit())
		})
	})
})
