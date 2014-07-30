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
			convergeTaskLog := c.logger.Session("converge-tasks", lager.Data{
				"expire-claimed-task-duration": c.expireClaimedTaskDuration,
				"kick-pending-task-duration":   c.kickPendingTaskDuration,
			})

			convergeLRPLog := c.logger.Session("converge-lrps")

			convergeLRPStartLog := c.logger.Session("converge-lrp-start-auctions", lager.Data{
				"expire-claimed-task-duration": c.expireClaimedLRPAuctionDuration,
				"kick-pending-task-duration":   c.kickPendingLRPAuctionDuration,
			})

			convergeLRPStopLog := c.logger.Session("converge-lrp-stop-auctions", lager.Data{
				"expire-claimed-task-duration": c.expireClaimedLRPAuctionDuration,
				"kick-pending-task-duration":   c.kickPendingLRPAuctionDuration,
			})

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()

				convergeTaskLog.Info("starting")
				defer convergeTaskLog.Info("finished")

				c.bbs.ConvergeTask(c.expireClaimedTaskDuration, c.kickPendingTaskDuration)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				convergeLRPLog.Info("starting")
				defer convergeLRPLog.Info("finished")

				c.bbs.ConvergeLRPs()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				convergeLRPStartLog.Info("starting")
				defer convergeLRPStartLog.Info("finished")

				c.bbs.ConvergeLRPStartAuctions(c.kickPendingLRPAuctionDuration, c.expireClaimedLRPAuctionDuration)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				convergeLRPStopLog.Info("starting")
				defer convergeLRPStopLog.Info("finished")

				c.bbs.ConvergeLRPStopAuctions(c.kickPendingLRPAuctionDuration, c.expireClaimedLRPAuctionDuration)
			}()

			wg.Wait()
		}
	}
}
