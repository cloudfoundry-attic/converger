package converger_process

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/nu7hatch/gouuid"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
)

type ConvergerProcess struct {
	id                                   string
	bbs                                  Bbs.ConvergerBBS
	logger                               *steno.Logger
	kickPendingTaskDuration              time.Duration
	expireClaimedTaskDuration            time.Duration
	kickPendingLRPStartAuctionDuration   time.Duration
	expireClaimedLRPStartAuctionDuration time.Duration
	closeOnce                            *sync.Once
}

func New(bbs Bbs.ConvergerBBS, logger *steno.Logger, kickPendingTaskDuration, expireClaimedTaskDuration, kickPendingLRPStartAuctionDuration, expireClaimedLRPStartAuctionDuration time.Duration) *ConvergerProcess {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}
	return &ConvergerProcess{
		id:                                   uuid.String(),
		bbs:                                  bbs,
		logger:                               logger,
		kickPendingTaskDuration:              kickPendingTaskDuration,
		expireClaimedTaskDuration:            expireClaimedTaskDuration,
		kickPendingLRPStartAuctionDuration:   kickPendingLRPStartAuctionDuration,
		expireClaimedLRPStartAuctionDuration: expireClaimedLRPStartAuctionDuration,
		closeOnce: &sync.Once{},
	}
}

func (c *ConvergerProcess) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	statusChannel, releaseLock, err := c.bbs.MaintainConvergeLock(c.kickPendingTaskDuration, c.id)
	if err != nil {
		c.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "error when creating converge lock")
		return err
	}

	close(ready)

	once := &sync.Once{}

	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				go func() {
					once.Do(func() {
						close(releaseLock)
					})
				}()
			}
		case locked, ok := <-statusChannel:
			if !ok {
				return nil
			}

			if locked {
				wg := sync.WaitGroup{}
				wg.Add(3)
				go func() {
					defer wg.Done()
					c.bbs.ConvergeTask(c.expireClaimedTaskDuration, c.kickPendingTaskDuration)
				}()
				go func() {
					defer wg.Done()
					c.bbs.ConvergeLRPs()
				}()
				go func() {
					defer wg.Done()
					c.bbs.ConvergeLRPStartAuctions(c.kickPendingLRPStartAuctionDuration, c.expireClaimedLRPStartAuctionDuration)
				}()
				wg.Wait()
			}
		}
	}
}
