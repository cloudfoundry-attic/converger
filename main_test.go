package main_test

import (
	"fmt"
	"syscall"
	"time"

	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/vito/cmdtest"
	. "github.com/vito/cmdtest/matchers"

	"github.com/cloudfoundry-incubator/converger/converger_runner"
)

var _ = Describe("Main", func() {
	var (
		etcdRunner *etcdstorerunner.ETCDClusterRunner
		bbs        *Bbs.BBS

		runner *converger_runner.ConvergerRunner
	)

	BeforeEach(func() {
		etcdPort := 5001 + config.GinkgoConfig.ParallelNode
		etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)

		etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1)
		etcdRunner.Start()

		bbs = Bbs.New(etcdRunner.Adapter(), timeprovider.NewTimeProvider())

		convergerPath, err := cmdtest.Build("github.com/cloudfoundry-incubator/converger", "-race")
		Ω(err).ShouldNot(HaveOccurred())

		runner = converger_runner.New(
			convergerPath,
			etcdCluster,
			"info",
			30*time.Second,
			30*time.Minute,
		)
	})

	Describe("when the converger receives the TERM signal", func() {
		BeforeEach(func() {
			runner.Start()
			runner.Session.Cmd.Process.Signal(syscall.SIGTERM)
		})

		It("exits successfully", func() {
			Ω(runner.Session).Should(ExitWithTimeout(0, 4*time.Second))
		})
	})

	AfterEach(func() {
		//		runner.KillWithFire()
		etcdRunner.Stop()
	})
})
