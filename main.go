package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"

	"github.com/cloudfoundry-incubator/converger/task_converger"
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

var convergenceInterval = flag.Duration(
	"convergenceInterval",
	30*time.Second,
	"the interval, in seconds, between convergences",
)

var timeToClaimTask = flag.Duration(
	"timeToClaimTask",
	30*time.Minute,
	"unclaimed run onces are marked as failed, after this time (in seconds)",
)

func main() {
	flag.Parse()

	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workerpool.NewWorkerPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		log.Fatalf("converger.etcd.connect: %s\n", err)
	}

	bbs := Bbs.NewConvergerBBS(etcdAdapter, timeprovider.NewTimeProvider())

	l, err := steno.GetLogLevel(*logLevel)
	if err != nil {
		log.Fatalf("Invalid loglevel: %s\n", *logLevel)
	}

	stenoConfig := steno.Config{
		Level: l,
		Sinks: []steno.Sink{steno.NewIOSink(os.Stdout)},
	}

	if *syslogName != "" {
		stenoConfig.Sinks = append(stenoConfig.Sinks, steno.NewSyslogSink(*syslogName))
	}

	steno.Init(&stenoConfig)
	logger := steno.NewLogger("executor")

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	c := task_converger.New(bbs, logger, *convergenceInterval, *timeToClaimTask)

	ready := make(chan struct{})
	go func() {
		<-ready
		fmt.Print("Converger started")
	}()

	err = c.Run(sigChan, ready)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
}
