package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

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

var logLevel = flag.String(
	"logLevel",
	"info",
	"the logging level (none, fatal, error, warn, info, debug, debug1, debug2, all)",
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

	logger := initializeLogger()
	bbs := initializeBbs(logger)

	convergerProcess := ifrit.Envoke(converger_process.New(
		bbs,
		logger,
		*convergeRepeatInterval,
		*kickPendingTaskDuration,
		*expireClaimedTaskDuration,
		*kickPendingLRPStartAuctionDuration,
		*expireClaimedLRPStartAuctionDuration,
	))

	logger.Info("converger.started")

	monitor := ifrit.Envoke(sigmon.New(convergerProcess))

	err := <-monitor.Wait()

	if err != nil {
		logger.Errord(map[string]interface{}{
			"error": err.Error(),
		}, "converger.exited")
		os.Exit(1)
	}
	logger.Info("converger.exited")
}

func initializeLogger() *steno.Logger {
	l, err := steno.GetLogLevel(*logLevel)
	if err != nil {
		log.Fatalf("Invalid loglevel: %s\n", *logLevel)
	}

	stenoConfig := &steno.Config{
		Level: l,
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
