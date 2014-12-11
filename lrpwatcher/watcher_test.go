package lrpwatcher_test

import (
	"errors"
	"syscall"

	. "github.com/cloudfoundry-incubator/converger/lrpwatcher"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Watcher", func() {
	var (
		sender *fake.FakeMetricSender

		bbs                   *fake_bbs.FakeConvergerBBS
		logger                *lagertest.TestLogger
		desiredLRP            models.DesiredLRP
		repAddrRelativeToCell string
		healthChecks          map[string]string

		watcher ifrit.Process

		desiredLRPChangeChan chan models.DesiredLRPChange
		desiredLRPStopChan   chan bool
		desiredLRPErrChan    chan error
	)

	BeforeEach(func() {
		bbs = new(fake_bbs.FakeConvergerBBS)

		repAddrRelativeToCell = "127.0.0.1:20515"

		healthChecks = map[string]string{
			"some-stack": "some-health-check.tgz",
		}

		logger = lagertest.NewTestLogger("test")

		sender = fake.NewFakeMetricSender()
		metrics.Initialize(sender)

		watcherRunner := New(bbs, logger)

		desiredLRP = models.DesiredLRP{
			Domain:      "some-domain",
			ProcessGuid: "the-app-guid-the-app-version",

			Instances: 2,
			Stack:     "some-stack",

			Action: &models.RunAction{
				Path: "some-run-action-path",
			},
		}

		desiredLRPChangeChan = make(chan models.DesiredLRPChange)
		desiredLRPStopChan = make(chan bool)
		desiredLRPErrChan = make(chan error)

		bbs.WatchForDesiredLRPChangesStub = func() (<-chan models.DesiredLRPChange, chan<- bool, <-chan error) {
			return desiredLRPChangeChan, desiredLRPStopChan, desiredLRPErrChan
		}

		watcher = ifrit.Invoke(watcherRunner)
	})

	AfterEach(func(done Done) {
		watcher.Signal(syscall.SIGINT)
		<-watcher.Wait()
		Eventually(desiredLRPStopChan).Should(BeClosed())
		close(done)
	})

	Describe("lifecycle", func() {
		Describe("waiting until all desired are processed before shutting down", func() {
			var receivedAuctions chan models.LRPStartAuction

			BeforeEach(func() {
				receivedAuctions = make(chan models.LRPStartAuction)
				bbs.RequestLRPStartAuctionStub = func(lrp models.LRPStartAuction) error {
					receivedAuctions <- lrp
					return nil
				}
			})

			It("should not shut down until all desireds are processed", func() {
				desiredLRPChangeChan <- models.DesiredLRPChange{
					Before: nil,
					After:  &desiredLRP,
				}

				desiredLRPChangeChan <- models.DesiredLRPChange{
					Before: nil,
					After:  &desiredLRP,
				}

				watcher.Signal(syscall.SIGINT)
				didShutDown := watcher.Wait()

				Consistently(didShutDown).ShouldNot(Receive())

				for i := 0; i < desiredLRP.Instances*2; i++ {
					Eventually(receivedAuctions).Should(Receive())
				}

				Eventually(didShutDown).Should(Receive())
			})
		})

		Describe("when an error occurs", func() {
			var newChan chan models.DesiredLRPChange
			BeforeEach(func() {
				newChan = make(chan models.DesiredLRPChange, 1)
				desiredLRPChangeChan = newChan
				desiredLRPErrChan <- errors.New("oops")
			})

			It("should reestablish the watch", func() {
				newChan <- models.DesiredLRPChange{
					Before: nil,
					After:  &desiredLRP,
				}

				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(2))
			})
		})

		Describe("when the desired channel is closed", func() {
			var newChan chan models.DesiredLRPChange
			BeforeEach(func() {
				newChan = make(chan models.DesiredLRPChange, 1)
				oldChan := desiredLRPChangeChan
				desiredLRPChangeChan = newChan
				close(oldChan)
			})

			It("should reestablish the watch", func() {
				newChan <- models.DesiredLRPChange{
					Before: nil,
					After:  &desiredLRP,
				}

				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(2))
			})
		})
	})

	Describe("when a desired LRP change message is received", func() {
		JustBeforeEach(func() {
			desiredLRPChangeChan <- models.DesiredLRPChange{
				Before: nil,
				After:  &desiredLRP,
			}
		})

		Describe("the happy path", func() {
			It("creates ActualLRPs for the desired LRP", func() {
				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(2))

				firstActual := bbs.CreateActualLRPArgsForCall(0)
				Ω(firstActual.ProcessGuid).Should(Equal("the-app-guid-the-app-version"))
				Ω(firstActual.Index).Should(Equal(0))
				Ω(firstActual.InstanceGuid).ShouldNot(BeEmpty())

				secondActual := bbs.CreateActualLRPArgsForCall(1)
				Ω(secondActual.ProcessGuid).Should(Equal("the-app-guid-the-app-version"))
				Ω(secondActual.Index).Should(Equal(1))
				Ω(secondActual.InstanceGuid).ShouldNot(BeEmpty())
			})

			It("requests a LRPStartAuction with the desired LRP", func() {
				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(2))

				firstStartAuction := bbs.RequestLRPStartAuctionArgsForCall(0)
				Ω(firstStartAuction.DesiredLRP.ProcessGuid).Should(Equal("the-app-guid-the-app-version"))
				Ω(firstStartAuction.InstanceGuid).ShouldNot(BeEmpty())

				secondStartAuction := bbs.RequestLRPStartAuctionArgsForCall(1)
				Ω(secondStartAuction.DesiredLRP.ProcessGuid).Should(Equal("the-app-guid-the-app-version"))
				Ω(secondStartAuction.InstanceGuid).ShouldNot(BeEmpty())

				Ω(firstStartAuction.InstanceGuid).ShouldNot(Equal(secondStartAuction.InstanceGuid))
			})

			It("assigns increasing indices for the auction requests", func() {
				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(2))

				firstStartAuction := bbs.RequestLRPStartAuctionArgsForCall(0)
				Ω(firstStartAuction.Index).Should(Equal(0))

				secondStartAuction := bbs.RequestLRPStartAuctionArgsForCall(1)
				Ω(secondStartAuction.Index).Should(Equal(1))
			})

			It("increases the lrp start counter", func() {
				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(2))
				Ω(sender.GetCounter("LRPStartIndexRequests")).Should(Equal(uint64(2)))
			})
		})

		Context("when there is an error creating the ActualLRP", func() {
			BeforeEach(func() {
				bbs.CreateActualLRPReturns(nil, errors.New("connection error"))
			})

			It("does not start an auction", func() {
				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(2))
				Ω(bbs.RequestLRPStartAuctionCallCount()).Should(Equal(0))
			})
		})

		Context("when there is an error requesting a LRPStartAuction", func() {
			BeforeEach(func() {
				bbs.RequestLRPStartAuctionReturns(errors.New("connection error"))
			})

			It("logs an error", func() {
				Eventually(logger.TestSink.Buffer).Should(gbytes.Say("watcher.desired-lrp-change.request-start-auction-failed"))
			})
		})

		Context("when there is an error fetching the actual instances", func() {
			BeforeEach(func() {
				bbs.ActualLRPsByProcessGuidReturns(nil, errors.New("connection error"))
			})

			It("does not request any LRPStartAuctions", func() {
				Consistently(bbs.CreateActualLRPCallCount).Should(BeZero())
				Consistently(bbs.RequestLRPStartAuctionCallCount).Should(BeZero())
			})
		})

		Context("when there missing indices and extra instances for the LRP", func() {
			BeforeEach(func() {
				desiredLRP.Instances = 4

				bbs.ActualLRPsByProcessGuidReturns([]models.ActualLRP{
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "a",
						Index:        0,
						State:        models.ActualLRPStateClaimed,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "b",
						Index:        4,
						State:        models.ActualLRPStateRunning,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "c",
						Index:        5,
						State:        models.ActualLRPStateUnclaimed,
					},
				}, nil)
			})

			It("starts missing ones", func() {
				Eventually(bbs.RequestLRPStartAuctionCallCount).Should(Equal(3))

				Ω(bbs.RequestLRPStartAuctionArgsForCall(0).Index).Should(Equal(1))
				Ω(bbs.RequestLRPStartAuctionArgsForCall(1).Index).Should(Equal(2))
				Ω(bbs.RequestLRPStartAuctionArgsForCall(2).Index).Should(Equal(3))
			})

			It("stops extra running instances and increases the lrp stop instance counter", func() {
				Eventually(bbs.RequestStopLRPInstancesCallCount).Should(Equal(1))
				Ω(bbs.RequestStopLRPInstancesArgsForCall(0)).Should(Equal([]models.ActualLRP{
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "b",
						Index:        4,
						State:        models.ActualLRPStateRunning,
					},
				}))

				Ω(sender.GetCounter("LRPStopInstanceRequests")).Should(Equal(uint64(1)))
			})

			It("removes extra unclaimed instances and increases the lrp remove instance counter", func() {
				Eventually(bbs.RemoveActualLRPCallCount).Should(Equal(1))
				Ω(bbs.RemoveActualLRPArgsForCall(0)).Should(Equal(models.ActualLRP{
					ProcessGuid:  "the-app-guid-the-app-version",
					InstanceGuid: "c",
					Index:        5,
					State:        models.ActualLRPStateUnclaimed,
				}))

				Ω(sender.GetCounter("LRPRemoveInstanceRequests")).Should(Equal(uint64(1)))
			})
		})

		Context("when there are duplicate instances per index running for the desired app", func() {
			BeforeEach(func() {
				desiredLRP.Instances = 3

				bbs.ActualLRPsByProcessGuidReturns([]models.ActualLRP{
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "a",
						Index:        0,
						State:        models.ActualLRPStateClaimed,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "b",
						Index:        1,
						State:        models.ActualLRPStateUnclaimed,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "c",
						Index:        1,
						State:        models.ActualLRPStateClaimed,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "d",
						Index:        2,
						State:        models.ActualLRPStateRunning,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "e",
						Index:        2,
						State:        models.ActualLRPStateRunning,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "f",
						Index:        3,
						State:        models.ActualLRPStateRunning,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "g",
						Index:        3,
						State:        models.ActualLRPStateRunning,
					},
				}, nil)
			})

			It("doesn't start anything", func() {
				Consistently(bbs.RequestLRPStartAuctionCallCount).Should(BeZero())
			})

			It("holds stop auctions for the duplicate instances per index, and extra indexes", func() {
				Eventually(bbs.RequestLRPStopAuctionCallCount).Should(Equal(2))

				Ω([]models.LRPStopAuction{
					bbs.RequestLRPStopAuctionArgsForCall(0),
					bbs.RequestLRPStopAuctionArgsForCall(1),
				}).Should(ConsistOf([]models.LRPStopAuction{
					{
						ProcessGuid: "the-app-guid-the-app-version",
						Index:       1,
					},
					{
						ProcessGuid: "the-app-guid-the-app-version",
						Index:       2,
					},
				}))
			})

			It("stops extra ones", func() {
				Eventually(bbs.RequestStopLRPInstancesCallCount).Should(Equal(1))

				Ω(bbs.RequestStopLRPInstancesArgsForCall(0)).Should(ConsistOf([]models.ActualLRP{
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "f",
						Index:        3,
						State:        models.ActualLRPStateRunning,
					},
					{
						ProcessGuid:  "the-app-guid-the-app-version",
						InstanceGuid: "g",
						Index:        3,
						State:        models.ActualLRPStateRunning,
					},
				}))
			})

			It("increases the lrp stop counter", func() {
				Eventually(bbs.RequestStopLRPInstancesCallCount).Should(Equal(1))
				Ω(sender.GetCounter("LRPStopIndexRequests")).Should(Equal(uint64(2)))
			})
		})
	})

	Describe("when a desired LRP is deleted", func() {
		JustBeforeEach(func() {
			desiredLRPChangeChan <- models.DesiredLRPChange{
				Before: &desiredLRP,
				After:  nil,
			}
		})

		BeforeEach(func() {
			bbs.ActualLRPsByProcessGuidReturns([]models.ActualLRP{
				{
					ProcessGuid:  "the-app-guid-the-app-version",
					InstanceGuid: "a",
					Index:        0,
					State:        models.ActualLRPStateClaimed,
				},
			}, nil)
		})

		It("doesn't start anything", func() {
			Consistently(bbs.RequestLRPStartAuctionCallCount).Should(BeZero())
		})

		It("stops all instances", func() {
			Eventually(bbs.RequestStopLRPInstancesCallCount).Should(Equal(1))

			Ω(bbs.RequestStopLRPInstancesArgsForCall(0)).Should(Equal([]models.ActualLRP{
				{
					ProcessGuid:  "the-app-guid-the-app-version",
					InstanceGuid: "a",
					Index:        0,
					State:        models.ActualLRPStateClaimed,
				},
			}))
		})
	})
})
