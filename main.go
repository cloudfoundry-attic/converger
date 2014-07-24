package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/cf-lager"
	"github.com/cloudfoundry-incubator/converger/converger_process"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/sigmon"
)

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
)

var syslogName = flag.String(
	"syslogName",
	"",
	"syslog name",
)

var convergeRepeatInterval = flag.Duration(
	"convergeRepeatInterval",
	30*time.Second,
	"the interval, in seconds, between runs of the converge process",
)

var kickPendingTaskDuration = flag.Duration(
	"kickPendingTaskDuration",
	30*time.Second,
	"the interval, in seconds, between kicks to pending tasks",
)

var expireClaimedTaskDuration = flag.Duration(
	"expireClaimedTaskDuration",
	30*time.Minute,
	"unclaimed tasks are marked as failed, after this time (in seconds)",
)

var kickPendingLRPStartAuctionDuration = flag.Duration(
	"kickPendingLRPStartAuctionDuration",
	30*time.Second,
	"the interval, in seconds, between kicks to pending start auctions for long-running process",
)

var expireClaimedLRPStartAuctionDuration = flag.Duration(
	"expireClaimedLRPStartAuctionDuration",
	300*time.Second,
	"unclaimed start auctions for long-running processes are deleted, after this time (in seconds)",
)

func main() {
	flag.Parse()

	bbs := initializeBbs(initializeStenoLogger())

	logger := cf_lager.New("converger")

	convergerProcess := ifrit.Envoke(converger_process.New(
		bbs,
		logger,
		*convergeRepeatInterval,
		*kickPendingTaskDuration,
		*expireClaimedTaskDuration,
		*kickPendingLRPStartAuctionDuration,
		*expireClaimedLRPStartAuctionDuration,
	))

	monitor := ifrit.Envoke(sigmon.New(convergerProcess))

	logger.Info("started")

	err := <-monitor.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func initializeStenoLogger() *steno.Logger {
	stenoConfig := &steno.Config{
		Sinks: []steno.Sink{steno.NewIOSink(os.Stdout)},
	}

	if *syslogName != "" {
		stenoConfig.Sinks = append(stenoConfig.Sinks, steno.NewSyslogSink(*syslogName))
	}

	steno.Init(stenoConfig)
	return steno.NewLogger("converger")
}

func initializeBbs(logger *steno.Logger) Bbs.ConvergerBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatalf("converger.etcd.connect: %s\n", err)
	}

	return Bbs.NewConvergerBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)
}
