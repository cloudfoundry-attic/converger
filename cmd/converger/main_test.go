package main_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"syscall"
	"time"

	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"github.com/cloudfoundry-incubator/bbs"
	bbsrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/bbs/models/test/model_helpers"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	convergerrunner "github.com/cloudfoundry-incubator/converger/cmd/converger/testrunner"
	"github.com/cloudfoundry-incubator/locket"
	"github.com/cloudfoundry-incubator/locket/presence"
)

type BinPaths struct {
	Converger string
	Bbs       string
}

var _ = Describe("Converger", func() {
	const (
		exitDuration                = 4 * time.Second
		convergeRepeatInterval      = 500 * time.Millisecond
		taskKickInterval            = convergeRepeatInterval
		expireCompletedTaskDuration = 3 * convergeRepeatInterval
		expirePendingTaskDuration   = 30 * time.Minute
	)

	var (
		binPaths         BinPaths
		etcdRunner       *etcdstorerunner.ETCDClusterRunner
		bbsArgs          bbsrunner.Args
		bbsProcess       ifrit.Process
		bbsClient        bbs.Client
		convergerConfig  *convergerrunner.Config
		convergerProcess ifrit.Process
		runner           *ginkgomon.Runner

		consulRunner  *consulrunner.ClusterRunner
		consulSession *consuladapter.Session

		etcdClient storeadapter.StoreAdapter

		logger lager.Logger
	)

	SynchronizedBeforeSuite(func() []byte {
		convergerBinPath, err := Build("github.com/cloudfoundry-incubator/converger/cmd/converger", "-race")
		Expect(err).NotTo(HaveOccurred())
		bbsBinPath, err := Build("github.com/cloudfoundry-incubator/bbs/cmd/bbs", "-race")
		Expect(err).NotTo(HaveOccurred())
		bytes, err := json.Marshal(BinPaths{
			Converger: convergerBinPath,
			Bbs:       bbsBinPath,
		})
		Expect(err).NotTo(HaveOccurred())
		return bytes
	}, func(bytes []byte) {
		binPaths = BinPaths{}
		err := json.Unmarshal(bytes, &binPaths)
		Expect(err).NotTo(HaveOccurred())

		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1, nil)

		etcdClient = etcdRunner.Adapter(nil)

		consulRunner = consulrunner.NewClusterRunner(
			9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
			1,
			"http",
		)

		logger = lagertest.NewTestLogger("test")

		bbsAddress := fmt.Sprintf("127.0.0.1:%d", 13000+GinkgoParallelNode())

		bbsURL := &url.URL{
			Scheme: "http",
			Host:   bbsAddress,
		}

		bbsArgs = bbsrunner.Args{
			Address:           bbsAddress,
			AdvertiseURL:      bbsURL.String(),
			AuctioneerAddress: "some-address",
			EtcdCluster:       etcdCluster,
			ConsulCluster:     consulRunner.ConsulCluster(),
		}

		convergerConfig = &convergerrunner.Config{
			BinPath:                     binPaths.Converger,
			ConvergeRepeatInterval:      convergeRepeatInterval.String(),
			KickTaskDuration:            taskKickInterval.String(),
			ExpirePendingTaskDuration:   expirePendingTaskDuration.String(),
			ExpireCompletedTaskDuration: expireCompletedTaskDuration.String(),
			ConsulCluster:               consulRunner.ConsulCluster(),
			LogLevel:                    "info",
			BBSAddress:                  bbsURL.String(),
		}
	})

	SynchronizedAfterSuite(func() {
	}, func() {
		CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()
		consulRunner.Start()
		consulRunner.WaitUntilReady()

		bbsProcess = ginkgomon.Invoke(bbsrunner.New(binPaths.Bbs, bbsArgs))
		bbsClient = bbs.NewClient(fmt.Sprint("http://", bbsArgs.Address))

		consulSession = consulRunner.NewSession("a-session")

		capacity := presence.NewCellCapacity(512, 1024, 124)
		cellPresence := presence.NewCellPresence("the-cell-id", "1.2.3.4", "the-zone", capacity, []string{}, []string{})

		value, err := presence.ToJSON(cellPresence)
		Expect(err).NotTo(HaveOccurred())

		_, err = consulSession.SetPresence(locket.CellSchemaPath(cellPresence.CellID), value)
		Expect(err).NotTo(HaveOccurred())

	})

	AfterEach(func() {
		ginkgomon.Kill(bbsProcess)
		ginkgomon.Kill(convergerProcess)
		consulRunner.Stop()
		etcdRunner.Stop()
	})

	startConverger := func() {
		runner = convergerrunner.New(convergerConfig)
		convergerProcess = ginkgomon.Invoke(runner)
		time.Sleep(convergeRepeatInterval)
	}

	createRunningTaskWithDeadCell := func() {
		task := model_helpers.NewValidTask("task-guid")

		err := bbsClient.DesireTask(task.TaskGuid, task.Domain, task.TaskDefinition)
		Expect(err).NotTo(HaveOccurred())

		_, err = bbsClient.StartTask(task.TaskGuid, "dead-cell")
		Expect(err).NotTo(HaveOccurred())
	}

	itIsInactive := func() {
		Describe("when a task is desired but its cell is dead", func() {
			JustBeforeEach(createRunningTaskWithDeadCell)

			It("does not converge the task", func() {
				Consistently(func() []*models.Task {
					return getTasksByState(bbsClient, models.Task_Completed)
				}, 10*convergeRepeatInterval).Should(BeEmpty())
			})
		})
	}

	Context("when the converger has the lock", func() {
		Describe("when a task is desired but its cell is dead", func() {
			JustBeforeEach(createRunningTaskWithDeadCell)

			It("marks the task as completed and failed", func() {
				Consistently(func() []*models.Task {
					return getTasksByState(bbsClient, models.Task_Completed)
				}, 0.5).Should(BeEmpty())

				startConverger()

				Eventually(func() []*models.Task {
					return getTasksByState(bbsClient, models.Task_Completed)
				}, 10*convergeRepeatInterval).Should(HaveLen(1))
			})
		})
	})

	Context("when the converger loses the lock", func() {
		BeforeEach(func() {
			startConverger()
			Eventually(runner, 5*time.Second).Should(gbytes.Say("acquire-lock-succeeded"))

			consulRunner.DestroySession("converger")
		})

		It("exits with an error", func() {
			Eventually(runner, exitDuration).Should(Exit(1))
		})
	})

	Context("when the converger initially does not have the lock", func() {
		var otherSession *consuladapter.Session

		BeforeEach(func() {
			otherSession = consulRunner.NewSession("other-session")
			err := otherSession.AcquireLock(locket.LockSchemaPath("converge_lock"), []byte("something-else"))
			Expect(err).NotTo(HaveOccurred())

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
					Eventually(func() []*models.Task {
						completedTasks := getTasksByState(bbsClient, models.Task_Completed)
						return failedTasks(completedTasks)
					}, 10*convergeRepeatInterval).Should(HaveLen(1))
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
				convergerProcess.Signal(syscall.SIGINT)
				Eventually(runner, exitDuration).Should(Exit(0))
			})
		})

		Describe("when it receives SIGTERM", func() {
			It("exits successfully", func() {
				convergerProcess.Signal(syscall.SIGTERM)
				Eventually(runner, exitDuration).Should(Exit(0))
			})
		})
	})

	Context("when etcd is down", func() {
		BeforeEach(func() {
			etcdRunner.Stop()
			startConverger()
		})

		It("starts", func() {
			Consistently(runner).ShouldNot(Exit())
		})
	})
})

func getTasksByState(client bbs.Client, state models.Task_State) []*models.Task {
	tasks, err := client.Tasks()
	Expect(err).NotTo(HaveOccurred())

	filteredTasks := make([]*models.Task, 0)
	for _, task := range tasks {
		if task.State == state {
			filteredTasks = append(filteredTasks, task)
		}
	}
	return filteredTasks
}

func failedTasks(tasks []*models.Task) []*models.Task {
	failedTasks := make([]*models.Task, 0)

	for _, task := range tasks {
		if task.Failed {
			failedTasks = append(failedTasks, task)
		}
	}

	return failedTasks
}
