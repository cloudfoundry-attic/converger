package converger_process

import (
	"os"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"

	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/locket"
	"github.com/pivotal-golang/clock"
)

type ConvergerProcess struct {
	id                          string
	locketClient                locket.Client
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
	locketClient locket.Client,
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
		locketClient:  locketClient,
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
	logger := c.logger.Session("converger-process")
	logger.Debug("started")
	convergeTimer := c.clock.NewTimer(c.convergeRepeatInterval)
	defer func() {
		logger.Debug("done")
		convergeTimer.Stop()
	}()

	cellDisappeared := make(chan locket.CellEvent)

	done := make(chan struct{})
	go func() {
		events := c.locketClient.CellEvents()
		for {
			select {
			case event := <-events:
				switch event.EventType() {
				case locket.CellDisappeared:
					logger.Info("received-cell-disappeared-event", lager.Data{"cell-id": event.CellIDs()})
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

		case <-cellDisappeared:
			c.converge()

		case <-convergeTimer.C():
			c.converge()
		}

		convergeTimer.Reset(c.convergeRepeatInterval)
	}
}

func (c *ConvergerProcess) converge() {
	logger := c.logger.Session("executing-convergence")
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		logger.Debug("converge-tasks-started")

		defer func() {
			logger.Debug("converge-tasks-done")
			wg.Done()
		}()

		err := c.bbsClient.ConvergeTasks(
			c.kickTaskDuration,
			c.expirePendingTaskDuration,
			c.expireCompletedTaskDuration,
		)
		if err != nil {
			logger.Error("failed-to-converge-tasks", err)
		}
	}()

	wg.Add(1)
	go func() {
		logger.Debug("converge-lrps-started")

		defer func() {
			logger.Debug("converge-lrps-done")
			wg.Done()
		}()

		err := c.bbsClient.ConvergeLRPs()
		if err != nil {
			logger.Error("failed-to-converge-lrps", err)
		}
	}()

	wg.Wait()
}
