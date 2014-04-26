package converger_runner

import (
	"os/exec"
	"syscall"
	"time"

	"github.com/cloudfoundry/gunk/runner_support"
	. "github.com/onsi/gomega"
	"github.com/vito/cmdtest"
)

type ConvergerRunner struct {
	binPath string
	Session *cmdtest.Session
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
	convergerSession, err := cmdtest.StartWrapped(
		exec.Command(
			r.binPath,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-convergenceInterval", r.config.convergenceInterval.String(),
			"-timeToClaimTask", r.config.timeToClaimTask.String(),
		),
		runner_support.TeeToGinkgoWriter,
		runner_support.TeeToGinkgoWriter,
	)
	Ω(err).ShouldNot(HaveOccurred())
	r.Session = convergerSession
}

func (r *ConvergerRunner) Stop() {
	if r.Session != nil {
		r.Session.Cmd.Process.Signal(syscall.SIGTERM)
		_, err := r.Session.Wait(5 * time.Second)
		Ω(err).ShouldNot(HaveOccurred())
	}
}

func (r *ConvergerRunner) KillWithFire() {
	if r.Session != nil {
		r.Session.Cmd.Process.Kill()
	}
}
