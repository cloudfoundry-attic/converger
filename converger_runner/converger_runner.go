package converger_runner

import (
	"os/exec"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

type ConvergerRunner struct {
	binPath string
	Session *gexec.Session
	config  Config
}

type Config struct {
	etcdCluster         string
	logLevel            string
	convergenceInterval time.Duration
	timeToClaimTask     time.Duration
}

func New(binPath, etcdCluster, logLevel string, convergenceInterval, timeToClaimTask time.Duration) *ConvergerRunner {
	return &ConvergerRunner{
		binPath: binPath,
		config: Config{
			etcdCluster:         etcdCluster,
			logLevel:            logLevel,
			convergenceInterval: convergenceInterval,
			timeToClaimTask:     timeToClaimTask,
		},
	}
}

func (r *ConvergerRunner) Start() {
	convergerSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-convergenceInterval", r.config.convergenceInterval.String(),
			"-timeToClaimTask", r.config.timeToClaimTask.String(),
		),
		GinkgoWriter,
		GinkgoWriter,
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = convergerSession
	Eventually(r.Session.Buffer()).Should(gbytes.Say("started"))
}

func (r *ConvergerRunner) Stop() {
	if r.Session != nil {
		r.Session.Command.Process.Signal(syscall.SIGTERM)
		r.Session.Wait(5 * time.Second)
	}
}

func (r *ConvergerRunner) KillWithFire() {
	if r.Session != nil {
		r.Session.Command.Process.Kill()
	}
}
