package lrpwatcher

import (
	"errors"
	"os"
	"sync"

	"github.com/cloudfoundry-incubator/delta_force/delta_force"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
)

var ErrNoHealthCheckDefined = errors.New("no health check defined for stack")

const (
	lrpStartIndexCounter   = metric.Counter("LRPStartIndexRequests")
	lrpStopIndexCounter    = metric.Counter("LRPStopIndexRequests")
	lrpStopInstanceCounter = metric.Counter("LRPStopInstanceRequests")
)

type Watcher struct {
	bbs    Bbs.ConvergerBBS
	logger lager.Logger
}

func New(bbs Bbs.ConvergerBBS, logger lager.Logger) Watcher {
	return Watcher{
		bbs:    bbs,
		logger: logger.Session("watcher"),
	}
}

func (watcher Watcher) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	wg := new(sync.WaitGroup)

	desiredChangeChan, stopChan, errChan := watcher.bbs.WatchForDesiredLRPChanges()

	close(ready)

	for {
		if desiredChangeChan == nil {
			desiredChangeChan, stopChan, errChan = watcher.bbs.WatchForDesiredLRPChanges()
		}

		select {
		case desiredChange, ok := <-desiredChangeChan:
			if ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					watcher.processDesiredChange(desiredChange)
				}()
			} else {
				watcher.logger.Error("watch-closed", nil)
				desiredChangeChan = nil
			}

		case err, ok := <-errChan:
			if ok {
				watcher.logger.Error("watch-error", err)
			}
			desiredChangeChan = nil

		case <-signals:
			watcher.logger.Info("shutting-down")
			close(stopChan)
			wg.Wait()
			watcher.logger.Info("shut-down")
			return nil
		}
	}

	return nil
}

type LRPIM interface {
	RequestLRPStartAuction(models.LRPStartAuction) error
	RequestStopLRPInstance(models.ActualLRP) error
	RequestLRPStopAuction(models.LRPStopAuction) error
}

type Banana interface {
	ApplyDelta(models.DesiredLRP, []models.ActualLRP) error
}

func (watcher Watcher) ProcessDesiredChange(desiredLRP models.DesiredLRP, actualInstances models.ActualLRPsByIndex) error {
	changeLogger := watcher.logger.Session("desired-lrp-change")

	delta := delta_force.Reconcile(desiredLRP.Instances, actualInstances)

	for _, lrpIndex := range delta.IndicesToStart {
		changeLogger.Info("request-start", lager.Data{
			"desired-app-message": desiredLRP,
			"index":               lrpIndex,
		})

		instanceGuid, err := uuid.NewV4()
		if err != nil {
			changeLogger.Error("generating-instance-guid-failed", err)
			return
		}

		startMessage := models.LRPStartAuction{
			DesiredLRP: desiredLRP,

			Index:        lrpIndex,
			InstanceGuid: instanceGuid.String(),
		}

		lrpStartIndexCounter.Increment()

		err = watcher.bbs.RequestLRPStartAuction(startMessage)
		if err != nil {
			changeLogger.Error("request-start-auction-failed", err, lager.Data{
				"desired-app-message": desiredLRP,
				"index":               lrpIndex,
			})
		}
	}

	for _, idx := range delta.IndicesToStop {
		changeLogger.Info("request-stop-instance", lager.Data{
			"desired-app-message": desiredLRP,
			"stop-instance-guid":  guidToStop,
		})

		actualToStop := actualInstances[idx]

		lrpStopInstanceCounter.Increment()

		err = watcher.bbs.RequestStopLRPInstance(actualToStop)
		if err != nil {
			changeLogger.Error("request-stop-instance-failed", err, lager.Data{
				"desired-app-message": desiredLRP,
				"stop-instance-guid":  guidToStop,
			})
		}
	}
}

func (watcher Watcher) actualsForProcessGuid(lrpGuid string) (delta_force.ActualInstances, map[string]models.ActualLRP, error) {
	actualInstances := delta_force.ActualInstances{}
	actualLRPs, err := watcher.bbs.ActualLRPsByProcessGuid(lrpGuid)
	instanceGuidToActual := map[string]models.ActualLRP{}

	if err != nil {
		return actualInstances, instanceGuidToActual, err
	}

	for _, actualLRP := range actualLRPs {
		actualInstances = append(actualInstances, delta_force.ActualInstance{actualLRP.Index, actualLRP.InstanceGuid})
		instanceGuidToActual[actualLRP.InstanceGuid] = actualLRP
	}

	return actualInstances, instanceGuidToActual, err
}
