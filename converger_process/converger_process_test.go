package converger_process_test

import (
	"errors"
	"time"

	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/clock/fakeclock"

	"github.com/cloudfoundry-incubator/converger/converger_process"
)

const aBit = 100 * time.Millisecond

var _ = Describe("ConvergerProcess", func() {
	var (
		fakeBBS                     *fake_bbs.FakeConvergerBBS
		logger                      *lagertest.TestLogger
		fakeClock                   *fakeclock.FakeClock
		convergeRepeatInterval      time.Duration
		kickPendingTaskDuration     time.Duration
		expirePendingTaskDuration   time.Duration
		expireCompletedTaskDuration time.Duration

		process ifrit.Process

		waitEvents chan<- services_bbs.CellEvent
		waitErrs   chan<- error
	)

	BeforeEach(func() {
		fakeBBS = new(fake_bbs.FakeConvergerBBS)
		logger = lagertest.NewTestLogger("test")
		fakeClock = fakeclock.NewFakeClock(time.Now())

		convergeRepeatInterval = 1 * time.Second

		kickPendingTaskDuration = 10 * time.Millisecond
		expirePendingTaskDuration = 30 * time.Second
		expireCompletedTaskDuration = 60 * time.Minute

		cellEvents := make(chan services_bbs.CellEvent, 100)
		errs := make(chan error, 100)

		waitEvents = cellEvents
		waitErrs = errs

		fakeBBS.WaitForCellEventStub = func() (services_bbs.CellEvent, error) {
			select {
			case e := <-cellEvents:
				return e, nil
			case err := <-errs:
				return nil, err
			}
		}
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(
			converger_process.New(
				fakeBBS,
				logger,
				fakeClock,
				convergeRepeatInterval,
				kickPendingTaskDuration,
				expirePendingTaskDuration,
				expireCompletedTaskDuration,
			),
		)
	})

	AfterEach(func() {
		ginkgomon.Interrupt(process)
		Eventually(process.Wait()).Should(Receive())
	})

	Describe("converging over time", func() {
		It("converges tasks, LRPs, and auctions when the lock is periodically reestablished", func() {
			fakeClock.Increment(convergeRepeatInterval + aBit)

			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			_, timeToClaim, convergenceInterval, timeToResolve := fakeBBS.ConvergeTasksArgsForCall(0)
			Ω(timeToClaim).Should(Equal(expirePendingTaskDuration))
			Ω(convergenceInterval).Should(Equal(kickPendingTaskDuration))
			Ω(timeToResolve).Should(Equal(expireCompletedTaskDuration))

			fakeClock.Increment(convergeRepeatInterval + aBit)

			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(2))

			_, timeToClaim, convergenceInterval, timeToResolve = fakeBBS.ConvergeTasksArgsForCall(1)
			Ω(timeToClaim).Should(Equal(expirePendingTaskDuration))
			Ω(convergenceInterval).Should(Equal(kickPendingTaskDuration))
			Ω(timeToResolve).Should(Equal(expireCompletedTaskDuration))
		})
	})

	Describe("converging when cells disappear", func() {
		It("converges tasks and LRPs immediately", func() {
			Consistently(fakeBBS.ConvergeTasksCallCount).Should(Equal(0))
			Consistently(fakeBBS.ConvergeLRPsCallCount).Should(Equal(0))

			waitEvents <- services_bbs.CellDisappearedEvent{
				Presence: models.CellPresence{
					CellID:     "some-cell-id",
					RepAddress: "some-rep-addr",
					Zone:       "autozone",
				},
			}

			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			_, timeToClaim, convergenceInterval, timeToResolve := fakeBBS.ConvergeTasksArgsForCall(0)
			Ω(timeToClaim).Should(Equal(expirePendingTaskDuration))
			Ω(convergenceInterval).Should(Equal(kickPendingTaskDuration))
			Ω(timeToResolve).Should(Equal(expireCompletedTaskDuration))

			waitEvents <- services_bbs.CellAppearedEvent{
				Presence: models.CellPresence{
					CellID:     "some-cell-id",
					RepAddress: "some-rep-addr",
					Zone:       "autozone",
				},
			}

			Consistently(fakeBBS.ConvergeTasksCallCount).Should(Equal(1))
			Consistently(fakeBBS.ConvergeLRPsCallCount).Should(Equal(1))

			waitErrs <- errors.New("whoopsie")

			waitEvents <- services_bbs.CellDisappearedEvent{
				Presence: models.CellPresence{
					CellID:     "some-cell-id",
					RepAddress: "some-rep-addr",
					Zone:       "autozone",
				},
			}

			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(2))
		})

		It("defers convergence to one full interval later", func() {
			fakeClock.Increment(convergeRepeatInterval - aBit)

			waitEvents <- services_bbs.CellDisappearedEvent{
				Presence: models.CellPresence{
					CellID:     "some-cell-id",
					RepAddress: "some-rep-addr",
					Zone:       "autozone",
				},
			}

			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			fakeClock.Increment(2 * aBit)

			Consistently(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Consistently(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			fakeClock.Increment(convergeRepeatInterval + aBit)
			Eventually(fakeBBS.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBS.ConvergeLRPsCallCount, aBit).Should(Equal(2))
		})
	})
})
