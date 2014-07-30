package converger_process

import (
	"os"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
)

type ConvergerProcess struct {
	id                              string
	bbs                             Bbs.ConvergerBBS
	logger                          lager.Logger
	convergeRepeatInterval          time.Duration
	kickPendingTaskDuration         time.Duration
	expireClaimedTaskDuration       time.Duration
	kickPendingLRPAuctionDuration   time.Duration
	expireClaimedLRPAuctionDuration time.Duration
	closeOnce                       *sync.Once
}

func New(
	bbs Bbs.ConvergerBBS,
	logger lager.Logger,
	convergeRepeatInterval,
	kickPendingTaskDuration,
	expireClaimedTaskDuration,
	kickPendingLRPAuctionDuration,
	expireClaimedLRPAuctionDuration time.Duration,
) *ConvergerProcess {

	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}

	return &ConvergerProcess{
		id:                              uuid.String(),
		bbs:                             bbs,
		logger:                          logger,
		convergeRepeatInterval:          convergeRepeatInterval,
		kickPendingTaskDuration:         kickPendingTaskDuration,
		expireClaimedTaskDuration:       expireClaimedTaskDuration,
		kickPendingLRPAuctionDuration:   kickPendingLRPAuctionDuration,
		expireClaimedLRPAuctionDuration: expireClaimedLRPAuctionDuration,
		closeOnce:                       &sync.Once{},
	}
}

func (c *ConvergerProcess) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	ticker := time.NewTicker(c.convergeRepeatInterval)

	close(ready)

	for {
		select {
		case <-signals:
			return nil

		case <-ticker.C:
			tickLog := c.logger.Session("converge-tick", lager.Data{
				"expire-claimed-task-duration": c.expireClaimedTaskDuration.String(),
				"kick-pending-task-duration":   c.kickPendingTaskDuration.String(),
			})

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()

				tickLog.Info("starting-tasks")
				defer tickLog.Info("finished-tasks")

				c.bbs.ConvergeTask(c.expireClaimedTaskDuration, c.kickPendingTaskDuration)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				tickLog.Info("starting-lrps")
				defer tickLog.Info("finished-lrps")

				c.bbs.ConvergeLRPs()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				tickLog.Info("starting-start-auctions")
				defer tickLog.Info("finished-start-auctions")

				c.bbs.ConvergeLRPStartAuctions(c.kickPendingLRPAuctionDuration, c.expireClaimedLRPAuctionDuration)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				tickLog.Info("starting-stop-auctions")
				defer tickLog.Info("finished-stop-auctions")

				c.bbs.ConvergeLRPStopAuctions(c.kickPendingLRPAuctionDuration, c.expireClaimedLRPAuctionDuration)
			}()

			wg.Wait()
		}
	}
}
