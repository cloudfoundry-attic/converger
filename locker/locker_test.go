package locker_test

import (
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/converger/locker"
)

var _ = Describe("Locker", func() {
	var fakeBBS *fake_bbs.FakeConvergerBBS
	var logger *lagertest.TestLogger
	var heartbeatInterval time.Duration

	var ranRunner chan struct{}
	var gotSignals chan os.Signal
	var runner ifrit.Runner

	var process ifrit.Process

	BeforeEach(func() {
		fakeBBS = fake_bbs.NewFakeConvergerBBS()

		logger = lagertest.NewTestLogger("test")

		ranRunner = make(chan struct{}, 1)

		gotSignals = make(chan os.Signal, 1)

		runner = ifrit.RunFunc(func(signals <-chan os.Signal, ready chan<- struct{}) error {
					ranRunner <- struct{}{}
			close(ready)
			gotSignals <- <-signals
			return nil
		})

		heartbeatInterval = 100 * time.Millisecond
	})

	JustBeforeEach(func() {
		process = ifrit.Envoke(&locker.LockedRunner{
			HeartbeatInterval: heartbeatInterval,
			BBS:               fakeBBS,
			Runner:            runner,
			Logger:            logger,
		})
	})

	AfterEach(func() {
		process.Signal(os.Interrupt)
		Eventually(process.Wait()).Should(Receive())
	})

	Context("when the lock can be established", func() {
		JustBeforeEach(func() {
			fakeBBS.ConvergeLockStatusChan <- true
		})

		It("runs the inner runner", func() {
			Eventually(ranRunner).Should(Receive())
		})

		It("doesn't start the freakin' runner every dang time we heartbeat", func() {
			Eventually(ranRunner).Should(Receive())

			fakeBBS.ConvergeLockStatusChan <- true
			Consistently(ranRunner).ShouldNot(Receive())

			fakeBBS.ConvergeLockStatusChan <- true
			Consistently(ranRunner).ShouldNot(Receive())

			fakeBBS.ConvergeLockStatusChan <- true
			Consistently(ranRunner).ShouldNot(Receive())
		})

		Context("and the inner runner exits", func() {
			disaster := errors.New("oh no!")

			BeforeEach(func() {
				runner = ifrit.RunFunc(func(signals <-chan os.Signal, ready chan<- struct{}) error {
					close(ready)
					return disaster
				})
			})

			It("forwards its exit result", func() {
				err := <-process.Wait()
				Ω(err).Should(Equal(disaster))
			})
		})

		Context("but then it gets lost", func() {
			JustBeforeEach(func() {
				fakeBBS.ConvergeLockStatusChan <- false
			})

			It("interrupts the inner process", func() {
				Eventually(gotSignals).Should(Receive(Equal(syscall.SIGINT)))
			})
		})

		Context("and then the locker is sent SIGINT", func() {
			JustBeforeEach(func() {
				process.Signal(syscall.SIGINT)
			})

			It("releases the lock", func() {
				Eventually(fakeBBS.ConvergeLockStopChan).Should(BeClosed())
			})

			It("forwards it to the process", func() {
				Eventually(gotSignals).Should(Receive(Equal(syscall.SIGINT)))
			})
		})

		Context("and then the locker is sent SIGTERM", func() {
			JustBeforeEach(func() {
				process.Signal(syscall.SIGTERM)
			})

			It("releases the lock", func() {
				Eventually(fakeBBS.ConvergeLockStopChan).Should(BeClosed())
			})

			It("forwards it to the process", func() {
				Eventually(gotSignals).Should(Receive(Equal(syscall.SIGTERM)))
			})

			Context("twice", func() {
				JustBeforeEach(func() {
					process.Signal(syscall.SIGTERM)
				})

				It("doesn't blow up", func() {
					Eventually(fakeBBS.ConvergeLockStopChan).Should(BeClosed())
				})
			})
		})

		Context("and the locker is sent an arbitrary signal (SIGUSR2)", func() {
			JustBeforeEach(func() {
				process.Signal(syscall.SIGUSR2)
			})

			It("forwards it to the process", func() {
				Eventually(gotSignals).Should(Receive(Equal(syscall.SIGUSR2)))
			})
		})
	})

	Context("when the lock cannot be established", func() {
		BeforeEach(func() {
			fakeBBS.SetMaintainConvergeLockError(storeadapter.ErrorKeyExists)
		})

		It("returns an error", func() {
			err := <-process.Wait()
			Ω(err).Should(HaveOccurred())
		})

		It("does not envoke the runner", func() {
			<-process.Wait()
			Ω(ranRunner).ShouldNot(Receive())
		})
	})
})
