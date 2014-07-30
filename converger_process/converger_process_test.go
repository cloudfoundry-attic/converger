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
	var expireClaimedTaskDuration time.Duration
	var kickPendingLRPStartAuctionDuration time.Duration
	var expireClaimedLRPStartAuctionDuration time.Duration

	var process ifrit.Process

	BeforeEach(func() {
		fakeBBS = fake_bbs.NewFakeConvergerBBS()
		logger = lagertest.NewTestLogger("test")

		convergeRepeatInterval = 1 * time.Second

		kickPendingTaskDuration = 10 * time.Millisecond
		expireClaimedTaskDuration = 30 * time.Second
		kickPendingLRPStartAuctionDuration = 30 * time.Second
		expireClaimedLRPStartAuctionDuration = 300 * time.Second

		process = ifrit.Envoke(converger_process.New(fakeBBS, logger, convergeRepeatInterval, kickPendingTaskDuration, expireClaimedTaskDuration, kickPendingLRPStartAuctionDuration, expireClaimedLRPStartAuctionDuration))
	})

	AfterEach(func() {
		process.Signal(syscall.SIGINT)
		Eventually(process.Wait()).Should(Receive())
	})

	It("converges tasks, LRPs, and auctions when the lock is periodically reestablished", func() {
		Eventually(fakeBBS.CallsToConvergeTasks, convergeRepeatInterval+2*aBit).Should(Equal(1))
		Eventually(fakeBBS.CallsToConvergeLRPs).Should(Equal(1))
		Eventually(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(1))
		Eventually(fakeBBS.CallsToConvergeLRPStopAuctions).Should(Equal(1))
		Ω(fakeBBS.ConvergeTimeToClaimTasks()).Should(Equal(30 * time.Second))

		Eventually(fakeBBS.CallsToConvergeTasks, convergeRepeatInterval+2*aBit).Should(Equal(2))
		Eventually(fakeBBS.CallsToConvergeLRPs).Should(Equal(2))
		Eventually(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(2))
		Eventually(fakeBBS.CallsToConvergeLRPStopAuctions).Should(Equal(2))
		Ω(fakeBBS.ConvergeTimeToClaimTasks()).Should(Equal(30 * time.Second))
	})
})
