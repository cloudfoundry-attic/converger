package testrunner

import (
	"os/exec"
	"time"

	"github.com/onsi/ginkgo"
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
	EtcdCluster   string
	ConsulCluster string
	LogLevel      string
	EtcdCertFile  string
	EtcdKeyFile   string
	EtcdCaFile    string
}

func New(binPath string, config Config) *ConvergerRunner {
	return &ConvergerRunner{
		binPath: binPath,
		config:  config,
	}
}

func (r *ConvergerRunner) Start(convergeRepeatInterval, kickTaskDuration, expirePendingTaskDuration, expireCompletedTaskDuration time.Duration) {
	if r.Session != nil {
		panic("starting two convergers!!!")
	}

	convergerSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-etcdCluster", r.config.EtcdCluster,
			"-logLevel", r.config.LogLevel,
			"-convergeRepeatInterval", convergeRepeatInterval.String(),
			"-kickTaskDuration", kickTaskDuration.String(),
			"-expirePendingTaskDuration", expirePendingTaskDuration.String(),
			"-expireCompletedTaskDuration", expireCompletedTaskDuration.String(),
			"-lockRetryInterval", "1s",
			"-consulCluster", r.config.ConsulCluster,
			"-etcdCertFile", r.config.EtcdCertFile,
			"-etcdKeyFile", r.config.EtcdKeyFile,
			"-etcdCaFile", r.config.EtcdCaFile,
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[94m[converger]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[94m[converger]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Expect(err).NotTo(HaveOccurred())
	r.Session = convergerSession
	Eventually(r.Session, 5*time.Second).Should(gbytes.Say("acquiring-lock"))
}

func (r *ConvergerRunner) Stop() {
	if r.Session != nil {
		r.Session.Interrupt().Wait(5 * time.Second)
		r.Session = nil
	}
}

func (r *ConvergerRunner) KillWithFire() {
	if r.Session != nil {
		r.Session.Kill().Wait(5 * time.Second)
		r.Session = nil
	}
}
