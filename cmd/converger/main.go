package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/converger/converger_process"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
)

var heartbeatInterval = flag.Duration(
	"heartbeatInterval",
	lock_bbs.HEARTBEAT_INTERVAL,
	"the interval between heartbeats to the lock",
)

var convergeRepeatInterval = flag.Duration(
	"convergeRepeatInterval",
	30*time.Second,
	"the interval between runs of the converge process",
)

var kickPendingTaskDuration = flag.Duration(
	"kickPendingTaskDuration",
	30*time.Second,
	"the interval, in seconds, between kicks to pending tasks",
)

var expireCompletedTaskDuration = flag.Duration(
	"expireCompletedTaskDuration",
	120*time.Second,
	"completed, unresolved tasks are deleted after this duration",
)

var expirePendingTaskDuration = flag.Duration(
	"expirePendingTaskDuration",
	30*time.Minute,
	"unclaimed tasks are marked as failed, after this duration",
)

var communicationTimeout = flag.Duration(
	"communicationTimeout",
	10*time.Second,
	"Timeout applied to all HTTP requests.",
)

const (
	dropsondeOrigin      = "converger"
	dropsondeDestination = "localhost:3457"
)

func main() {
	cf_debug_server.AddFlags(flag.CommandLine)
	cf_lager.AddFlags(flag.CommandLine)
	flag.Parse()

	cf_http.Initialize(*communicationTimeout)

	logger := cf_lager.New("converger")

	initializeDropsonde(logger)

	bbs := initializeBBS(logger)

	uuid, err := uuid.NewV4()
	if err != nil {
		logger.Fatal("Couldn't generate uuid", err)
	}

	heartbeater := bbs.NewConvergeLock(uuid.String(), *heartbeatInterval)

	converger := converger_process.New(
		bbs,
		logger,
		timeprovider.NewTimeProvider(),
		*convergeRepeatInterval,
		*kickPendingTaskDuration,
		*expirePendingTaskDuration,
		*expireCompletedTaskDuration,
	)

	members := grouper.Members{
		{"heartbeater", heartbeater},
		{"converger", converger},
	}

	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine); dbgAddr != "" {
		members = append(grouper.Members{
			{"debug-server", cf_debug_server.Runner(dbgAddr)},
		}, members...)
	}

	group := grouper.NewOrdered(os.Interrupt, members)

	logger.Info("started-waiting-for-lock")

	process := ifrit.Invoke(sigmon.New(group))

	logger.Info("acquired-lock")

	err = <-process.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func initializeBBS(logger lager.Logger) Bbs.ConvergerBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workpool.NewWorkPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("failed-to-connect-to-etcd", err)
	}

	return Bbs.NewConvergerBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)
}

func initializeDropsonde(logger lager.Logger) {
	err := dropsonde.Initialize(dropsondeDestination, dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}
