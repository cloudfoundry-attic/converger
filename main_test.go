package main_test

import (
	"fmt"
	"syscall"
	"time"

	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/cloudfoundry-incubator/converger/converger_runner"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

var _ = Describe("Main", func() {
	var (
		etcdRunner      *etcdstorerunner.ETCDClusterRunner
		bbs             *Bbs.BBS
		runner          *converger_runner.ConvergerRunner
		convergeTimeout = 1 * time.Second
	)

	BeforeSuite(func() {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)

		logSink := steno.NewTestingSink()

		steno.Init(&steno.Config{
			Sinks: []steno.Sink{logSink},
		})

		logger := steno.NewLogger("the-logger")
		steno.EnterTestMode()
		bbs = Bbs.NewBBS(etcdRunner.Adapter(), timeprovider.NewTimeProvider(), logger)

		convergerBinPath, err := gexec.Build("github.com/cloudfoundry-incubator/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())

		runner = converger_runner.New(convergerBinPath, etcdCluster, "info")
	})

	AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
	})

	BeforeEach(func() {
		etcdRunner.Start()
	})

	AfterEach(func() {
		runner.KillWithFire()
		etcdRunner.Stop()
	})

	Context("when the converger is running", func() {
		BeforeEach(func() {
			runner.Start(convergeTimeout, 30*time.Minute)
			time.Sleep(10 * time.Millisecond)
			Ω(runner.Session.ExitCode()).Should(Equal(-1))
		})

		Context("and a claimed task with a dead executor is present", func() {
			BeforeEach(func() {
				task := models.Task{
					Guid: "task-guid",
				}

				task, err := bbs.DesireTask(task)
				Ω(err).ShouldNot(HaveOccurred())
				task, err = bbs.ClaimTask(task, "dead-executor")
				Ω(err).ShouldNot(HaveOccurred())

				time.Sleep(convergeTimeout + 20*time.Millisecond)
			})

			It("marks the task as failed", func() {
				tasks, err := bbs.GetAllTasks()
				Ω(err).ShouldNot(HaveOccurred())
				Ω(tasks).Should(HaveLen(1))
				Ω(tasks[0].State).Should(Equal(models.TaskStateCompleted))
				Ω(tasks[0].Failed).Should(BeTrue())
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
})
