package stop_auction_bbs

import (
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/prune"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

const (
	convergeLrpStopCounter     = metric.Counter("converge-lrp-stop-auction")
	pruneInvalidLrpStopCounter = metric.Counter("prune-invalid-lrp-stop-auction")
	pruneClaimedLrpStopCounter = metric.Counter("prune-claimed-lrp-stop-auction")
	pruneStopFailedCounter     = metric.Counter("prune-stop-auction-failed")
	casLrpStopCounter          = metric.Counter("compare-and-swap-lrp-stop-auction")
)

type compareAndSwappableLRPStopAuction struct {
	OldIndex          uint64
	NewLRPStopAuction models.LRPStopAuction
}

func (bbs *StopAuctionBBS) ConvergeLRPStopAuctions(kickPendingDuration time.Duration, expireClaimedDuration time.Duration) {
	convergeLrpStopCounter.Increment()
	auctionsToCAS := []compareAndSwappableLRPStopAuction{}

	err := prune.Prune(bbs.store, shared.LRPStopAuctionSchemaRoot, func(auctionNode storeadapter.StoreNode) (shouldKeep bool) {
		auction, err := models.NewLRPStopAuctionFromJSON(auctionNode.Value)
		if err != nil {
			bbs.logger.Info("detected-invalid-stop-auction-json", lager.Data{
				"error":   err.Error(),
				"payload": auctionNode.Value,
			})
			pruneInvalidLrpStopCounter.Increment()
			return false
		}

		updatedAt := time.Unix(0, auction.UpdatedAt)

		switch auction.State {
		case models.LRPStopAuctionStatePending:
			if bbs.timeProvider.Time().Sub(updatedAt) > kickPendingDuration {
				bbs.logger.Info("detected-pending-auction", lager.Data{
					"auction":       auction,
					"kick-duration": kickPendingDuration,
				})

				auctionsToCAS = append(auctionsToCAS, compareAndSwappableLRPStopAuction{
					OldIndex:          auctionNode.Index,
					NewLRPStopAuction: auction,
				})
			}

		case models.LRPStopAuctionStateClaimed:
			if bbs.timeProvider.Time().Sub(updatedAt) > expireClaimedDuration {
				bbs.logger.Info("detected-expired-claim", lager.Data{
					"auction":             auction,
					"expiration-duration": expireClaimedDuration,
				})
				pruneClaimedLrpStopCounter.Increment()
				return false
			}
		}

		return true
	})

	if err != nil {
		pruneStopFailedCounter.Increment()
		bbs.logger.Error("failed-to-prune-stop-auctions", err)
		return
	}

	casLrpStopCounter.Add(uint64(len(auctionsToCAS)))
	bbs.batchCompareAndSwapLRPStopAuctions(auctionsToCAS)
}

func (bbs *StopAuctionBBS) batchCompareAndSwapLRPStopAuctions(auctionsToCAS []compareAndSwappableLRPStopAuction) {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(auctionsToCAS))
	for _, auctionToCAS := range auctionsToCAS {
		auction := auctionToCAS.NewLRPStopAuction
		newStoreNode := storeadapter.StoreNode{
			Key:   shared.LRPStopAuctionSchemaPath(auction),
			Value: auction.ToJSON(),
		}

		go func(auctionToCAS compareAndSwappableLRPStopAuction, newStoreNode storeadapter.StoreNode) {
			err := bbs.store.CompareAndSwapByIndex(auctionToCAS.OldIndex, newStoreNode)
			if err != nil {
				bbs.logger.Error("failed-to-compare-and-swap", err)
			}

			waitGroup.Done()
		}(auctionToCAS, newStoreNode)
	}

	waitGroup.Wait()
}
