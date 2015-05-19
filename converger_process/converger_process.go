package converger_process

import (
	"os"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"

	"github.com/cloudfoundry-incubator/consuladapter"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/pivotal-golang/clock"
)

type ConvergerProcess struct {
	id                          string
	bbs                         Bbs.ConvergerBBS
	consulSession               *consuladapter.Session
	logger                      lager.Logger
	clock                       clock.Clock
	convergeRepeatInterval      time.Duration
	kickPendingTaskDuration     time.Duration
	expirePendingTaskDuration   time.Duration
	expireCompletedTaskDuration time.Duration
	closeOnce                   *sync.Once
}

func New(
	bbs Bbs.ConvergerBBS,
	consulSession *consuladapter.Session,
	logger lager.Logger,
	clock clock.Clock,
	convergeRepeatInterval,
	kickPendingTaskDuration,
	expirePendingTaskDuration,
	expireCompletedTaskDuration time.Duration,
) *ConvergerProcess {

	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}

	return &ConvergerProcess{
		id:            uuid.String(),
		bbs:           bbs,
		consulSession: consulSession,
		logger:        logger,
		clock:         clock,
		convergeRepeatInterval:      convergeRepeatInterval,
		kickPendingTaskDuration:     kickPendingTaskDuration,
		expirePendingTaskDuration:   expirePendingTaskDuration,
		expireCompletedTaskDuration: expireCompletedTaskDuration,
		closeOnce:                   &sync.Once{},
	}
}

func (c *ConvergerProcess) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	convergeTimer := c.clock.NewTimer(c.convergeRepeatInterval)
	defer convergeTimer.Stop()

	cellDisappeared := make(chan services_bbs.CellEvent)

	logger := c.logger.WithData(lager.Data{
		"expire-pending-task-duration": c.expirePendingTaskDuration.String(),
		"kick-pending-task-duration":   c.kickPendingTaskDuration.String(),
	})

	done := make(chan struct{})
	go func() {
		events := c.bbs.CellEvents()
		for {
			select {
			case event := <-events:
				switch event.EventType() {
				case services_bbs.CellDisappeared:
					c.logger.Info("received-cell-disappeared-event", lager.Data{"cell-id": event.CellIDs()})
					select {
					case cellDisappeared <- event:
					case <-done:
						return
					}
				}

			case <-done:
				return
			}
		}
	}()

	close(ready)

	for {
		select {
		case <-signals:
			close(done)
			return nil

		case event := <-cellDisappeared:
			c.converge(logger.Session("cell-disappeared", lager.Data{"cell-id": event.CellIDs()}))

		case <-convergeTimer.C():
			c.converge(logger.Session("converge-tick"))
		}

		convergeTimer.Reset(c.convergeRepeatInterval)
	}
}

func (c *ConvergerProcess) converge(tickLog lager.Logger) {
	cellsLoader := c.bbs.NewCellsLoader()
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.bbs.ConvergeTasks(
			tickLog,
			c.expirePendingTaskDuration,
			c.kickPendingTaskDuration,
			c.expireCompletedTaskDuration,
			cellsLoader,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.bbs.ConvergeLRPs(tickLog, cellsLoader)
	}()

	wg.Wait()
}
