package task_converger

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/nu7hatch/gouuid"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
)

type TaskConverger struct {
	id          string
	bbs         Bbs.ConvergerBBS
	logger      *steno.Logger
	interval    time.Duration
	timeToClaim time.Duration
	closeOnce   *sync.Once
}

func New(bbs Bbs.ConvergerBBS, logger *steno.Logger, interval, timeToClaim time.Duration) *TaskConverger {
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to generate a random guid....:" + err.Error())
	}
	return &TaskConverger{
		id:          uuid.String(),
		bbs:         bbs,
		logger:      logger,
		interval:    interval,
		timeToClaim: timeToClaim,
		closeOnce:   &sync.Once{},
	}
}

func (c *TaskConverger) Run(sigChan chan os.Signal, ready chan struct{}) error {
	statusChannel, releaseLock, err := c.bbs.MaintainConvergeLock(c.interval, c.id)
	if err != nil {
		c.logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "error when creating converge lock")
		return err
	}

	if ready != nil {
		close(ready)
	}

	firstTime := true

	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				done := make(chan bool)
				releaseLock <- done
				<-done
				return nil
			}
		case locked, ok := <-statusChannel:
			if !ok {
				return nil
			}

			if locked {
				if !firstTime {
					c.bbs.ConvergeTask(c.timeToClaim)
				}
				firstTime = false
			}
		}
	}
}
