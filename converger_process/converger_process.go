package converger_process

import (
	"os"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"

	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/consuladapter"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/services_bbs"
	"github.com/pivotal-golang/clock"
)

type ConvergerProcess struct {
	id                          string
	oldbbs                      Bbs.ConvergerBBS
	bbsClient                   bbs.Client
	consulSession               *consuladapter.Session
	logger                      lager.Logger
	clock                       clock.Clock
	convergeRepeatInterval      time.Duration
	kickTaskDuration            time.Duration
	expirePendingTaskDuration   time.Duration
	expireCompletedTaskDuration time.Duration
	closeOnce                   *sync.Once
}

func New(
	oldBbs Bbs.ConvergerBBS,
	bbsClient bbs.Client,
	consulSession *consuladapter.Session,
	logger lager.Logger,
	clock clock.Clock,
	convergeRepeatInterval,
	kickTaskDuration,
	expirePendingTaskDuration,
	expireCompletedTaskDuration time.Duration,
) *ConvergerProcess {

	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}

	return &ConvergerProcess{
		id:            uuid.String(),
		oldbbs:        oldBbs,
		bbsClient:     bbsClient,
		consulSession: consulSession,
		logger:        logger,
		clock:         clock,
		convergeRepeatInterval:      convergeRepeatInterval,
		kickTaskDuration:            kickTaskDuration,
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
		"kick-task-duration":             c.kickTaskDuration.String(),
		"expire-pending-task-duration":   c.expirePendingTaskDuration.String(),
		"expire-completed-task-duration": c.expireCompletedTaskDuration.String(),
	})

	done := make(chan struct{})
	go func() {
		events := c.oldbbs.CellEvents()
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
	cellsLoader := c.oldbbs.NewCellsLoader()
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.bbsClient.ConvergeTasks(
			tickLog,
			c.kickTaskDuration,
			c.expirePendingTaskDuration,
			c.expireCompletedTaskDuration,
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.oldbbs.ConvergeLRPs(tickLog, cellsLoader)
	}()

	wg.Wait()
}
