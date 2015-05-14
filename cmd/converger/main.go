package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/cf-debug-server"
	cf_lager "github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/cloudfoundry-incubator/consuladapter"
	"github.com/cloudfoundry-incubator/converger/converger_process"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
)

var receptorTaskHandlerURL = flag.String(
	"receptorTaskHandlerURL",
	"http://127.0.0.1:1169",
	"location of receptor task handler",
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd URLs (scheme://ip:port)",
)

var consulCluster = flag.String(
	"consulCluster",
	"",
	"comma-separated list of consul server URLs (scheme://ip:port)",
)

var lockTTL = flag.Duration(
	"lockTTL",
	lock_bbs.LockTTL,
	"TTL for service lock",
)

var lockRetryInterval = flag.Duration(
	"lockRetryInterval",
	lock_bbs.RetryInterval,
	"interval to wait before retrying a failed lock acquisition",
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

	logger, reconfigurableSink := cf_lager.New("converger")

	initializeDropsonde(logger)

	client, err := consuladapter.NewClient(*consulCluster)
	if err != nil {
		logger.Fatal("new-client-failed", err)
	}

	sessionMgr := consuladapter.NewSessionManager(client)
	consulSession, err := consuladapter.NewSession("converger", *lockTTL, client, sessionMgr)
	if err != nil {
		logger.Fatal("consul-session-failed", err)
	}

	convergerBBS := initializeConvergerBBS(logger, consulSession)

	uuid, err := uuid.NewV4()
	if err != nil {
		logger.Fatal("Couldn't generate uuid", err)
	}

	lockMaintainer := convergerBBS.NewConvergeLock(uuid.String(), *lockRetryInterval)

	converger := converger_process.New(
		convergerBBS,
		consulSession,
		logger,
		clock.NewClock(),
		*convergeRepeatInterval,
		*kickPendingTaskDuration,
		*expirePendingTaskDuration,
		*expireCompletedTaskDuration,
	)

	members := grouper.Members{
		{"lock-maintainer", lockMaintainer},
		{"converger", converger},
	}

	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine); dbgAddr != "" {
		members = append(grouper.Members{
			{"debug-server", cf_debug_server.Runner(dbgAddr, reconfigurableSink)},
		}, members...)
	}

	group := grouper.NewOrdered(os.Interrupt, members)

	process := ifrit.Invoke(sigmon.New(group))

	logger.Info("started")

	err = <-process.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func initializeConvergerBBS(logger lager.Logger, session *consuladapter.Session) bbs.ConvergerBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workpool.NewWorkPool(bbs.ConvergerBBSWorkPoolSize),
	)

	return bbs.NewConvergerBBS(etcdAdapter, session, *receptorTaskHandlerURL, clock.NewClock(), logger)
}

func initializeDropsonde(logger lager.Logger) {
	err := dropsonde.Initialize(dropsondeDestination, dropsondeOrigin)
	if err != nil {
		logger.Error("failed to initialize dropsonde: %v", err)
	}
}
