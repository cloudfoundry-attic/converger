package converger_process_test

import (
	"syscall"
	"time"

	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/converger/converger_process"
)

const aBit = 100 * time.Millisecond

var _ = Describe("ConvergerProcess", func() {
	var fakeBBS *fake_bbs.FakeConvergerBBS
	var logger *lagertest.TestLogger
	var convergeRepeatInterval time.Duration
	var kickPendingTaskDuration time.Duration
	var expirePendingTaskDuration time.Duration
	var expireCompletedTaskDuration time.Duration
	var kickPendingLRPStartAuctionDuration time.Duration
	var expireClaimedLRPStartAuctionDuration time.Duration

	var process ifrit.Process

	BeforeEach(func() {
		fakeBBS = new(fake_bbs.FakeConvergerBBS)
		logger = lagertest.NewTestLogger("test")

		convergeRepeatInterval = 1 * time.Second

		kickPendingTaskDuration = 10 * time.Millisecond
		expirePendingTaskDuration = 30 * time.Second
		expireCompletedTaskDuration = 60 * time.Minute
		kickPendingLRPStartAuctionDuration = 30 * time.Second
		expireClaimedLRPStartAuctionDuration = 300 * time.Second

		process = ifrit.Invoke(
			converger_process.New(
				fakeBBS,
				logger,
				convergeRepeatInterval,
				kickPendingTaskDuration,
				expirePendingTaskDuration,
				expireCompletedTaskDuration,
				kickPendingLRPStartAuctionDuration,
				expireClaimedLRPStartAuctionDuration))
	})

	AfterEach(func() {
		process.Signal(syscall.SIGINT)
		Eventually(process.Wait()).Should(Receive())
	})

	It("converges tasks, LRPs, and auctions when the lock is periodically reestablished", func() {
		Eventually(fakeBBS.ConvergeTaskCallCount, convergeRepeatInterval+2*aBit).Should(Equal(1))
		Eventually(fakeBBS.ConvergeLRPsCallCount).Should(Equal(1))
		Eventually(fakeBBS.ConvergeLRPStartAuctionsCallCount).Should(Equal(1))
		Eventually(fakeBBS.ConvergeLRPStopAuctionsCallCount).Should(Equal(1))

		timeToClaim, _, _ := fakeBBS.ConvergeTaskArgsForCall(0)
		Ω(timeToClaim).Should(Equal(30 * time.Second))

		Eventually(fakeBBS.ConvergeTaskCallCount, convergeRepeatInterval+2*aBit).Should(Equal(2))
		Eventually(fakeBBS.ConvergeLRPsCallCount).Should(Equal(2))
		Eventually(fakeBBS.ConvergeLRPStartAuctionsCallCount).Should(Equal(2))
		Eventually(fakeBBS.ConvergeLRPStopAuctionsCallCount).Should(Equal(2))
		timeToClaim, _, _ = fakeBBS.ConvergeTaskArgsForCall(0)
		Ω(timeToClaim).Should(Equal(30 * time.Second))
	})
})
