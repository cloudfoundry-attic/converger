package lrpreprocessor

import (
	"strconv"
	"strings"

	"github.com/cloudfoundry-incubator/runtime-schema/models"
)

type LRPreProcessor struct {
}

const (
	INSTANCE_INDEX_PLACEHOLDER_NAME = "PLACEHOLDER_INSTANCE_INDEX"
	INSTANCE_GUID_PLACEHOLDER_NAME  = "PLACEHOLDER_INSTANCE_GUID"
)

func New() *LRPreProcessor {
	return &LRPreProcessor{}
}

func (lrpp *LRPreProcessor) PreProcess(lrp models.DesiredLRP, index int, guid string) (models.DesiredLRP, error) {
	newActions := make([]models.ExecutorAction, len(lrp.Actions))
	for i, a := range lrp.Actions {
		newActions[i] = walk(a, index, guid)
	}

	lrp.Actions = newActions

	return lrp, nil
}

func walk(
	action models.ExecutorAction,
	index int,
	guid string,
) models.ExecutorAction {
	switch v := action.Action.(type) {
	case models.DownloadAction:
		action.Action = v

	case models.TryAction:
		v.Action = walk(v.Action, index, guid)
		action.Action = v

	case models.MonitorAction:
		v.Action = walk(v.Action, index, guid)

		v.HealthyHook.URL = strings.Replace(v.HealthyHook.URL, INSTANCE_INDEX_PLACEHOLDER_NAME, strconv.Itoa(index), -1)
		v.HealthyHook.URL = strings.Replace(v.HealthyHook.URL, INSTANCE_GUID_PLACEHOLDER_NAME, guid, -1)

		v.UnhealthyHook.URL = strings.Replace(v.UnhealthyHook.URL, INSTANCE_INDEX_PLACEHOLDER_NAME, strconv.Itoa(index), -1)
		v.UnhealthyHook.URL = strings.Replace(v.UnhealthyHook.URL, INSTANCE_GUID_PLACEHOLDER_NAME, guid, -1)

		action.Action = v

	case models.EmitProgressAction:
		v.Action = walk(v.Action, index, guid)
		action.Action = v

	case models.ParallelAction:
		newActions := make([]models.ExecutorAction, len(v.Actions))
		for i, a := range v.Actions {
			newActions[i] = walk(a, index, guid)
		}

		v.Actions = newActions

		action.Action = v
	}

	return action
}
