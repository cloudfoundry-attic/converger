package lrpwatcher_test

import (
	"errors"
	"syscall"

	. "github.com/cloudfoundry-incubator/converger/lrpwatcher"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/fake_bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/dropsonde/metric_sender/fake"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
			var createdActualLRP chan struct{}

			BeforeEach(func() {
				createdActualLRP = make(chan struct{})
				bbs.CreateActualLRPStub = func(models.DesiredLRP, int, lager.Logger) (*models.ActualLRP, error) {
					createdActualLRP <- struct{}{}
					return nil, nil
				}
			})

			It("should not shut down until all desireds are processed", func() {
				numChanges := 2

				for i := 0; i < numChanges; i++ {
					desiredLRPChangeChan <- models.DesiredLRPChange{
						Before: nil,
						After:  &desiredLRP,
					}
				}

				watcher.Signal(syscall.SIGINT)
				didShutDown := watcher.Wait()

				Consistently(didShutDown).ShouldNot(Receive())

				for i := 0; i < desiredLRP.Instances*numChanges; i++ {
					Eventually(createdActualLRP).Should(Receive())
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

				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(2))
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

				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(2))
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

				firstDesired, firstIndex, _ := bbs.CreateActualLRPArgsForCall(0)
				Ω(firstDesired).Should(Equal(desiredLRP))
				Ω(firstIndex).Should(Equal(0))

				secondDesired, secondIndex, _ := bbs.CreateActualLRPArgsForCall(1)
				Ω(secondDesired).Should(Equal(desiredLRP))
				Ω(secondIndex).Should(Equal(1))
			})

			It("increases the lrp start counter", func() {
				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(2))
				Ω(sender.GetCounter("LRPStartIndexRequests")).Should(Equal(uint64(2)))
			})
		})

		Context("when there is an error fetching the actual instances", func() {
			BeforeEach(func() {
				bbs.ActualLRPsByProcessGuidReturns(nil, errors.New("connection error"))
			})

			It("does not create any LRPStartAuctions", func() {
				Consistently(bbs.CreateActualLRPCallCount).Should(BeZero())
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
				Eventually(bbs.CreateActualLRPCallCount).Should(Equal(3))

				_, index0, _ := bbs.CreateActualLRPArgsForCall(0)
				Ω(index0).Should(Equal(1))

				_, index1, _ := bbs.CreateActualLRPArgsForCall(1)
				Ω(index1).Should(Equal(2))

				_, index2, _ := bbs.CreateActualLRPArgsForCall(2)
				Ω(index2).Should(Equal(3))
			})

			It("stops extra running instances and increases the lrp stop instance counter", func() {
				Eventually(bbs.RetireActualLRPsCallCount).Should(Equal(1))

				retiredLRPs, _ := bbs.RetireActualLRPsArgsForCall(0)
				Ω(retiredLRPs).Should(Equal([]models.ActualLRP{
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
				}))

				Ω(sender.GetCounter("LRPInstanceStopRequests")).Should(Equal(uint64(2)))
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
			Consistently(bbs.CreateActualLRPCallCount).Should(BeZero())
		})

		It("stops all instances", func() {
			Eventually(bbs.RetireActualLRPsCallCount).Should(Equal(1))

			retiredLRPs, _ := bbs.RetireActualLRPsArgsForCall(0)
			Ω(retiredLRPs).Should(Equal([]models.ActualLRP{
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
