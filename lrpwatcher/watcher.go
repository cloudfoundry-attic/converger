package lrpwatcher

import (
	"os"
	"sync"

	"github.com/cloudfoundry-incubator/delta_force/delta_force"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/metric"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

const (
	lrpStartInstanceCounter = metric.Counter("LRPInstanceStartRequests")
	lrpStopInstanceCounter  = metric.Counter("LRPInstanceStopRequests")
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

func (watcher Watcher) processDesiredChange(desiredChange models.DesiredLRPChange) {
	var desiredLRP models.DesiredLRP
	var desiredInstances int

	changeLogger := watcher.logger.Session("desired-lrp-change", lager.Data{
		"desired-lrp": desiredLRP,
	})

	if desiredChange.After == nil {
		desiredLRP = *desiredChange.Before
		desiredInstances = 0
	} else {
		desiredLRP = *desiredChange.After
		desiredInstances = desiredLRP.Instances
	}

	actuals, err := watcher.bbs.ActualLRPsByProcessGuid(desiredLRP.ProcessGuid)
	if err != nil {
		changeLogger.Error("fetch-actuals-failed", err, lager.Data{"desired-app-message": desiredLRP})
		return
	}

	delta := delta_force.Reconcile(desiredInstances, actuals)

	for _, lrpIndex := range delta.IndicesToStart {
		changeLogger.Info("request-start", lager.Data{
			"index": lrpIndex,
		})

		lrpStartInstanceCounter.Increment()
		err = watcher.bbs.CreateActualLRP(desiredLRP, lrpIndex, changeLogger)
		if err != nil {
			changeLogger.Error("failed-to-create-actual-lrp", err, lager.Data{
				"index": lrpIndex,
			})
		}
	}

	lrpsToRetire := []models.ActualLRP{}
	for _, index := range delta.IndicesToStop {
		changeLogger.Info("request-stop-instance", lager.Data{
			"stop-index": index,
		})

		lrpsToRetire = append(lrpsToRetire, actuals[index])
	}

	lrpStopInstanceCounter.Add(uint64(len(lrpsToRetire)))

	err = watcher.bbs.RetireActualLRPs(lrpsToRetire, changeLogger)
	if err != nil {
		changeLogger.Error("failed-to-retire-actual-lrps", err)
	}
}
