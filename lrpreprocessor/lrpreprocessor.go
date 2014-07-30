package lrpreprocessor

import (
	"strconv"
	"strings"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type LRPreProcessor struct {
	bbs bbs.ConvergerBBS
}

const (
	FILESERVER_URL_PLACEHOLDER_NAME = "PLACEHOLDER_FILESERVER_URL"
	INSTANCE_INDEX_PLACEHOLDER_NAME = "PLACEHOLDER_INSTANCE_INDEX"
	INSTANCE_GUID_PLACEHOLDER_NAME  = "PLACEHOLDER_INSTANCE_GUID"
)

func New(bbs bbs.ConvergerBBS) *LRPreProcessor {
	return &LRPreProcessor{
		bbs: bbs,
	}
}

func (lrpp *LRPreProcessor) PreProcess(lrp models.DesiredLRP, index int, guid string) (models.DesiredLRP, error) {
	fileserverURL, err := lrpp.bbs.GetAvailableFileServer()
	if err != nil {
		return models.DesiredLRP{}, err
	}

	newActions := make([]models.ExecutorAction, len(lrp.Actions))
	for i, a := range lrp.Actions {
		newActions[i] = walk(a, fileserverURL, index, guid)
	}

	lrp.Actions = newActions

	return lrp, nil
}

func walk(
	action models.ExecutorAction,
	fileserverURL string,
	index int,
	guid string,
) models.ExecutorAction {
	switch v := action.Action.(type) {
	case models.DownloadAction:
		v.From = strings.Replace(v.From, FILESERVER_URL_PLACEHOLDER_NAME+"/", fileserverURL, -1)
		action.Action = v

	case models.TryAction:
		v.Action = walk(v.Action, fileserverURL, index, guid)
		action.Action = v

	case models.MonitorAction:
		v.Action = walk(v.Action, fileserverURL, index, guid)

		v.HealthyHook.URL = strings.Replace(v.HealthyHook.URL, INSTANCE_INDEX_PLACEHOLDER_NAME, strconv.Itoa(index), -1)
		v.HealthyHook.URL = strings.Replace(v.HealthyHook.URL, INSTANCE_GUID_PLACEHOLDER_NAME, guid, -1)

		v.UnhealthyHook.URL = strings.Replace(v.UnhealthyHook.URL, INSTANCE_INDEX_PLACEHOLDER_NAME, strconv.Itoa(index), -1)
		v.UnhealthyHook.URL = strings.Replace(v.UnhealthyHook.URL, INSTANCE_GUID_PLACEHOLDER_NAME, guid, -1)

		action.Action = v

	case models.EmitProgressAction:
		v.Action = walk(v.Action, fileserverURL, index, guid)
		action.Action = v

	case models.ParallelAction:
		newActions := make([]models.ExecutorAction, len(v.Actions))
		for i, a := range v.Actions {
			newActions[i] = walk(a, fileserverURL, index, guid)
		}

		v.Actions = newActions

		action.Action = v
	}

	return action
}
