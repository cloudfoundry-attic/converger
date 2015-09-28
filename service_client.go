package converger

import (
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/locket"
	"github.com/cloudfoundry-incubator/locket/maintainer"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
)

const ConvergerLockSchemaKey = "converge_lock"

func ConvergerLockSchemaPath() string {
	return locket.LockSchemaPath(ConvergerLockSchemaKey)
}

type ServiceClient interface {
	NewConvergerLockRunner(logger lager.Logger, convergerID string, retryInterval time.Duration) ifrit.Runner
}

type serviceClient struct {
	session *consuladapter.Session
	clock   clock.Clock
}

func NewServiceClient(session *consuladapter.Session, clock clock.Clock) ServiceClient {
	return serviceClient{session, clock}
}

func (c serviceClient) NewConvergerLockRunner(logger lager.Logger, convergerID string, retryInterval time.Duration) ifrit.Runner {
	return maintainer.NewLock(c.session, ConvergerLockSchemaPath(), []byte(convergerID), c.clock, retryInterval, logger)
}
