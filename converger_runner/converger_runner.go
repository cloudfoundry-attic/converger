package converger_runner

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/cloudfoundry-incubator/consuladapter"
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
	etcdCluster string
	consulPort  int
	logLevel    string
}

func New(binPath, etcdCluster string, consulPort int, logLevel string) *ConvergerRunner {
	return &ConvergerRunner{
		binPath: binPath,
		config: Config{
			etcdCluster: etcdCluster,
			consulPort:  consulPort,
			logLevel:    logLevel,
		},
	}
}

func (r *ConvergerRunner) Start(convergeRepeatInterval, kickPendingTaskDuration, expirePendingTaskDuration, expireCompletedTaskDuration time.Duration) {
	if r.Session != nil {
		panic("starting two convergers!!!")
	}

	convergerSession, err := gexec.Start(
		exec.Command(
			r.binPath,
			"-etcdCluster", r.config.etcdCluster,
			"-logLevel", r.config.logLevel,
			"-convergeRepeatInterval", convergeRepeatInterval.String(),
			"-kickPendingTaskDuration", kickPendingTaskDuration.String(),
			"-expirePendingTaskDuration", expirePendingTaskDuration.String(),
			"-expireCompletedTaskDuration", expireCompletedTaskDuration.String(),
			"-heartbeatRetryInterval", "1s",
			"-consulCluster", fmt.Sprintf("127.0.0.1:%d", r.config.consulPort+consuladapter.PortOffsetHTTP),
			"-consulScheme", "http",
		),
		gexec.NewPrefixedWriter("\x1b[32m[o]\x1b[94m[converger]\x1b[0m ", ginkgo.GinkgoWriter),
		gexec.NewPrefixedWriter("\x1b[91m[e]\x1b[94m[converger]\x1b[0m ", ginkgo.GinkgoWriter),
	)

	Ω(err).ShouldNot(HaveOccurred())
	r.Session = convergerSession
	Eventually(r.Session, 5*time.Second).Should(gbytes.Say("started"))
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
