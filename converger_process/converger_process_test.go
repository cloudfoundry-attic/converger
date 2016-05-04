package converger_process_test

import (
	"errors"
	"time"

	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"github.com/cloudfoundry-incubator/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/clock/fakeclock"

	"github.com/cloudfoundry-incubator/converger/converger_process"
)

const aBit = 100 * time.Millisecond

var _ = Describe("ConvergerProcess", func() {
	var (
		fakeBBSServiceClient        *fake_bbs.FakeServiceClient
		fakeBBSClient               *fake_bbs.FakeInternalClient
		logger                      *lagertest.TestLogger
		fakeClock                   *fakeclock.FakeClock
		convergeRepeatInterval      time.Duration
		kickTaskDuration            time.Duration
		expirePendingTaskDuration   time.Duration
		expireCompletedTaskDuration time.Duration

		process ifrit.Process

		waitEvents chan<- models.CellEvent
		waitErrs   chan<- error
	)

	BeforeEach(func() {
		fakeBBSServiceClient = new(fake_bbs.FakeServiceClient)
		fakeBBSClient = new(fake_bbs.FakeInternalClient)
		logger = lagertest.NewTestLogger("test")
		fakeClock = fakeclock.NewFakeClock(time.Now())

		convergeRepeatInterval = 1 * time.Second

		kickTaskDuration = 10 * time.Millisecond
		expirePendingTaskDuration = 30 * time.Second
		expireCompletedTaskDuration = 60 * time.Minute

		cellEvents := make(chan models.CellEvent, 100)
		errs := make(chan error, 100)

		waitEvents = cellEvents
		waitErrs = errs

		fakeBBSServiceClient.CellEventsReturns(cellEvents)
	})

	JustBeforeEach(func() {
		process = ifrit.Invoke(
			converger_process.New(
				fakeBBSServiceClient,
				fakeBBSClient,
				logger,
				fakeClock,
				convergeRepeatInterval,
				kickTaskDuration,
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

			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			_, actualKickTaskDuration, actualExpirePendingTaskDuration, actualExpireCompletedTaskDuration := fakeBBSClient.ConvergeTasksArgsForCall(0)
			Expect(actualKickTaskDuration).To(Equal(kickTaskDuration))
			Expect(actualExpirePendingTaskDuration).To(Equal(expirePendingTaskDuration))
			Expect(actualExpireCompletedTaskDuration).To(Equal(expireCompletedTaskDuration))

			fakeClock.Increment(convergeRepeatInterval + aBit)

			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(2))

			_, actualKickTaskDuration, actualExpirePendingTaskDuration, actualExpireCompletedTaskDuration = fakeBBSClient.ConvergeTasksArgsForCall(1)
			Expect(actualKickTaskDuration).To(Equal(kickTaskDuration))
			Expect(actualExpirePendingTaskDuration).To(Equal(expirePendingTaskDuration))
			Expect(actualExpireCompletedTaskDuration).To(Equal(expireCompletedTaskDuration))
		})
	})

	Describe("converging when cells disappear", func() {
		It("converges tasks and LRPs immediately", func() {
			Consistently(fakeBBSClient.ConvergeTasksCallCount).Should(Equal(0))
			Consistently(fakeBBSClient.ConvergeLRPsCallCount).Should(Equal(0))

			waitEvents <- models.CellDisappearedEvent{
				IDs: []string{"some-cell-id"},
			}

			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			_, actualKickTaskDuration, actualExpirePendingTaskDuration, actualExpireCompletedTaskDuration := fakeBBSClient.ConvergeTasksArgsForCall(0)
			Expect(actualKickTaskDuration).To(Equal(kickTaskDuration))
			Expect(actualExpirePendingTaskDuration).To(Equal(expirePendingTaskDuration))
			Expect(actualExpireCompletedTaskDuration).To(Equal(expireCompletedTaskDuration))

			waitErrs <- errors.New("whoopsie")

			waitEvents <- models.CellDisappearedEvent{
				IDs: []string{"some-cell-id"},
			}

			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(2))
		})

		It("defers convergence to one full interval later", func() {
			fakeClock.Increment(convergeRepeatInterval - aBit)

			waitEvents <- models.CellDisappearedEvent{
				IDs: []string{"some-cell-id"},
			}

			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			fakeClock.Increment(2 * aBit)

			Consistently(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(1))
			Consistently(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(1))

			fakeClock.Increment(convergeRepeatInterval + aBit)
			Eventually(fakeBBSClient.ConvergeTasksCallCount, aBit).Should(Equal(2))
			Eventually(fakeBBSClient.ConvergeLRPsCallCount, aBit).Should(Equal(2))
		})
	})
})
