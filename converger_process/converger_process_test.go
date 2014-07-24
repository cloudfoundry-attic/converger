package converger_process_test

import (
	"syscall"
	"time"

	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/converger/converger_process"
)

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
		convergeRepeatInterval = 10 * time.Millisecond
		kickPendingTaskDuration = 10 * time.Millisecond
		expireClaimedTaskDuration = 30 * time.Second
		kickPendingLRPStartAuctionDuration = 30 * time.Second
		expireClaimedLRPStartAuctionDuration = 300 * time.Second
	})

	Context("when the lock can be established", func() {
		BeforeEach(func() {
			process = ifrit.Envoke(converger_process.New(fakeBBS, logger, convergeRepeatInterval, kickPendingTaskDuration, expireClaimedTaskDuration, kickPendingLRPStartAuctionDuration, expireClaimedLRPStartAuctionDuration))
		})

		AfterEach(func() {
			process.Signal(syscall.SIGINT)
			Eventually(fakeBBS.ConvergeLockStopChan).Should(BeClosed())
			close(fakeBBS.ConvergeLockStatusChan)
			Eventually(process.Wait()).Should(Receive())
		})

		It("converges tasks, LRPs, and auctions when the lock is periodically reestablished", func() {
			Consistently(fakeBBS.CallsToConvergeTasks).Should(Equal(0))
			Consistently(fakeBBS.CallsToConvergeLRPs).Should(Equal(0))
			Consistently(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(0))

			fakeBBS.ConvergeLockStatusChan <- true
			Eventually(fakeBBS.CallsToConvergeTasks).Should(Equal(1))
			Eventually(fakeBBS.CallsToConvergeLRPs).Should(Equal(1))
			Eventually(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(1))
			Eventually(fakeBBS.CallsToConvergeLRPStopAuctions).Should(Equal(1))
			Ω(fakeBBS.ConvergeTimeToClaimTasks()).Should(Equal(30 * time.Second))

			fakeBBS.ConvergeLockStatusChan <- true
			Eventually(fakeBBS.CallsToConvergeTasks).Should(Equal(2))
			Eventually(fakeBBS.CallsToConvergeLRPs).Should(Equal(2))
			Eventually(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(2))
			Eventually(fakeBBS.CallsToConvergeLRPStopAuctions).Should(Equal(2))
			Ω(fakeBBS.ConvergeTimeToClaimTasks()).Should(Equal(30 * time.Second))
		})
	})

	Context("when the lock cannot be established", func() {
		BeforeEach(func() {
			fakeBBS.SetMaintainConvergeLockError(storeadapter.ErrorKeyExists)
			process = ifrit.Envoke(converger_process.New(fakeBBS, logger, convergeRepeatInterval, kickPendingTaskDuration, expireClaimedTaskDuration, kickPendingLRPStartAuctionDuration, expireClaimedLRPStartAuctionDuration))
		})

		It("returns an error", func() {
			err := <-process.Wait()
			Ω(err).Should(HaveOccurred())
		})

		It("should not attempt to converge", func() {
			Consistently(fakeBBS.CallsToConvergeTasks).Should(Equal(0))
			Consistently(fakeBBS.CallsToConvergeLRPs).Should(Equal(0))
			Consistently(fakeBBS.CallsToConvergeLRPStartAuctions).Should(Equal(0))
			Consistently(fakeBBS.CallsToConvergeLRPStopAuctions).Should(Equal(0))
		})
	})
})
