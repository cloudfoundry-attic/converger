package fake_bbs

import (
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/fake_runner"
)

type FakeAuctioneerBBS struct {
	*sync.Mutex

	LRPStartAuctionChan      chan models.LRPStartAuction
	LRPStartAuctionStopChan  chan bool
	LRPStartAuctionErrorChan chan error

	LRPStopAuctionChan      chan models.LRPStopAuction
	LRPStopAuctionStopChan  chan bool
	LRPStopAuctionErrorChan chan error

	LockChannel        chan bool
	ReleaseLockChannel chan chan bool
	LockError          error

	ClaimedLRPStartAuctions   []models.LRPStartAuction
	ClaimLRPStartAuctionError error

	ResolvedLRPStartAuction     models.LRPStartAuction
	ResolveLRPStartAuctionError error

	ClaimedLRPStopAuctions   []models.LRPStopAuction
	ClaimLRPStopAuctionError error

	ResolvedLRPStopAuction     models.LRPStopAuction
	ResolveLRPStopAuctionError error

	Executors []models.ExecutorPresence
}

func NewFakeAuctioneerBBS() *FakeAuctioneerBBS {
	return &FakeAuctioneerBBS{
		Mutex:                    &sync.Mutex{},
		LRPStartAuctionChan:      make(chan models.LRPStartAuction),
		LRPStartAuctionStopChan:  make(chan bool),
		LRPStartAuctionErrorChan: make(chan error),
		LRPStopAuctionChan:       make(chan models.LRPStopAuction),
		LRPStopAuctionStopChan:   make(chan bool),
		LRPStopAuctionErrorChan:  make(chan error),
		LockChannel:              make(chan bool),
		ReleaseLockChannel:       make(chan chan bool),
	}
}

func (bbs *FakeAuctioneerBBS) NewAuctioneerLock(auctioneerID string, interval time.Duration) ifrit.Runner {
	panic("unimplemented")
	return new(fake_runner.FakeRunner)
}

func (bbs *FakeAuctioneerBBS) GetAllExecutors() ([]models.ExecutorPresence, error) {
	bbs.Lock()
	defer bbs.Unlock()
	return bbs.Executors, nil
}

func (bbs *FakeAuctioneerBBS) WatchForLRPStartAuction() (<-chan models.LRPStartAuction, chan<- bool, <-chan error) {
	bbs.Lock()
	defer bbs.Unlock()

	return bbs.LRPStartAuctionChan, bbs.LRPStartAuctionStopChan, bbs.LRPStartAuctionErrorChan
}

func (bbs *FakeAuctioneerBBS) ClaimLRPStartAuction(auction models.LRPStartAuction) error {
	bbs.Lock()
	defer bbs.Unlock()

	bbs.ClaimedLRPStartAuctions = append(bbs.ClaimedLRPStartAuctions, auction)
	return bbs.ClaimLRPStartAuctionError
}

func (bbs *FakeAuctioneerBBS) ResolveLRPStartAuction(auction models.LRPStartAuction) error {
	bbs.Lock()
	defer bbs.Unlock()

	bbs.ResolvedLRPStartAuction = auction
	return bbs.ResolveLRPStartAuctionError
}

func (bbs *FakeAuctioneerBBS) GetClaimedLRPStartAuctions() []models.LRPStartAuction {
	bbs.Lock()
	defer bbs.Unlock()
	return bbs.ClaimedLRPStartAuctions
}

func (bbs *FakeAuctioneerBBS) GetResolvedLRPStartAuction() models.LRPStartAuction {
	bbs.Lock()
	defer bbs.Unlock()
	return bbs.ResolvedLRPStartAuction
}

func (bbs *FakeAuctioneerBBS) WatchForLRPStopAuction() (<-chan models.LRPStopAuction, chan<- bool, <-chan error) {
	bbs.Lock()
	defer bbs.Unlock()

	return bbs.LRPStopAuctionChan, bbs.LRPStopAuctionStopChan, bbs.LRPStopAuctionErrorChan
}

func (bbs *FakeAuctioneerBBS) ClaimLRPStopAuction(auction models.LRPStopAuction) error {
	bbs.Lock()
	defer bbs.Unlock()

	bbs.ClaimedLRPStopAuctions = append(bbs.ClaimedLRPStopAuctions, auction)
	return bbs.ClaimLRPStopAuctionError
}

func (bbs *FakeAuctioneerBBS) ResolveLRPStopAuction(auction models.LRPStopAuction) error {
	bbs.Lock()
	defer bbs.Unlock()

	bbs.ResolvedLRPStopAuction = auction
	return bbs.ResolveLRPStopAuctionError
}

func (bbs *FakeAuctioneerBBS) GetClaimedLRPStopAuctions() []models.LRPStopAuction {
	bbs.Lock()
	defer bbs.Unlock()
	return bbs.ClaimedLRPStopAuctions
}

func (bbs *FakeAuctioneerBBS) GetResolvedLRPStopAuction() models.LRPStopAuction {
	bbs.Lock()
	defer bbs.Unlock()
	return bbs.ResolvedLRPStopAuction
}
