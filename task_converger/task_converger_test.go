package task_converger_test

import (
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry/storeadapter"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	steno "github.com/cloudfoundry/gosteno"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/converger/task_converger"
)

var _ = Describe("TaskConverger", func() {
	var fakeBBS *fake_bbs.FakeConvergerBBS
	var logger *steno.Logger
	var convergeInterval time.Duration
	var timeToClaim time.Duration
	var sigChan chan os.Signal

	var taskConverger *task_converger.TaskConverger

	BeforeEach(func() {
		fakeBBS = fake_bbs.NewFakeConvergerBBS()
		logger = steno.NewLogger("test-logger")
		convergeInterval = 10 * time.Millisecond
		timeToClaim = 30 * time.Second

		taskConverger = task_converger.New(fakeBBS, logger, convergeInterval, timeToClaim)
		sigChan = make(chan os.Signal)
	})

	Context("when running normally", func() {
		BeforeEach(func() {
			go taskConverger.Run(sigChan, nil)
		})
		AfterEach(func() {
			sigChan <- syscall.SIGINT
		})

		It("converges tasks on a regular interval", func() {
			Eventually(fakeBBS.CallsToConverge, 1.0, 0.1).Should(BeNumerically(">", 2))
			Ω(fakeBBS.ConvergeTimeToClaimTasks()).Should(Equal(30 * time.Second))
		})
	})

	Context("when signalled to stop", func() {
		BeforeEach(func() {
			go taskConverger.Run(sigChan, nil)
			time.Sleep(convergeInterval + convergeInterval/2)
			sigChan <- syscall.SIGINT
		})

		It("stops convergence when told", func() {
			time.Sleep(convergeInterval*2 + time.Millisecond)
			totalCalls := fakeBBS.CallsToConverge()
			Ω(totalCalls).Should(Equal(1))
		})
	})

	Context("when the converge lock cannot be acquired", func() {
		var err error

		BeforeEach(func() {
			fakeBBS.SetMaintainConvergeLockError(storeadapter.ErrorKeyExists)

			errChan := make(chan error, 1)
			runToEnd := func() bool {
				errChan <- taskConverger.Run(sigChan, nil)
				return true
			}

			Eventually(runToEnd, 1).Should(BeTrue())
			err = <-errChan
		})

		It("returns an error", func() {
			Ω(err).Should(HaveOccurred())
		})

		It("should only converge if it has the lock", func() {
			Consistently(fakeBBS.CallsToConverge).Should(Equal(0))
		})

		It("logs an error message when GrabLock fails", func() {
			testSink := steno.GetMeTheGlobalTestSink()

			records := []*steno.Record{}

			lockMessageIndex := 0
			Eventually(func() string {
				records = testSink.Records()

				if len(records) > 0 {
					lockMessageIndex := len(records) - 1
					return records[lockMessageIndex].Message
				}

				return ""
			}, 1.0, 0.1).Should(Equal("error when creating converge lock"))

			Ω(records[lockMessageIndex].Level).Should(Equal(steno.LOG_ERROR))
		})
	})
})
