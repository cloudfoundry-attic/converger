package main_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	bbsrunner "github.com/cloudfoundry-incubator/bbs/cmd/bbs/testrunner"
	"github.com/cloudfoundry-incubator/consuladapter/consulrunner"
	convergerrunner "github.com/cloudfoundry-incubator/converger/cmd/converger/testrunner"
	"github.com/cloudfoundry/storeadapter/storerunner/etcdstorerunner"
	"github.com/cloudfoundry/storeadapter/storerunner/mysqlrunner"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"

	"testing"
)

const (
	convergeRepeatInterval      = 500 * time.Millisecond
	taskKickInterval            = convergeRepeatInterval
	expireCompletedTaskDuration = 3 * convergeRepeatInterval
	expirePendingTaskDuration   = 30 * time.Minute
)

var (
	binPaths        BinPaths
	etcdRunner      *etcdstorerunner.ETCDClusterRunner
	bbsArgs         bbsrunner.Args
	consulRunner    *consulrunner.ClusterRunner
	convergerConfig *convergerrunner.Config
	logger          lager.Logger

	mySQLProcess ifrit.Process
	mySQLRunner  *mysqlrunner.MySQLRunner
	useSQL       bool
)

func TestConverger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Converger Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	convergerBinPath, err := Build("github.com/cloudfoundry-incubator/converger/cmd/converger", "-race")
	Expect(err).NotTo(HaveOccurred())
	bbsBinPath, err := Build("github.com/cloudfoundry-incubator/bbs/cmd/bbs", "-race")
	Expect(err).NotTo(HaveOccurred())
	bytes, err := json.Marshal(BinPaths{
		Converger: convergerBinPath,
		Bbs:       bbsBinPath,
	})
	Expect(err).NotTo(HaveOccurred())
	return bytes
}, func(bytes []byte) {
	useSQL = os.Getenv("USE_SQL") != ""
	binPaths = BinPaths{}
	err := json.Unmarshal(bytes, &binPaths)
	Expect(err).NotTo(HaveOccurred())

	etcdPort := 5001 + config.GinkgoConfig.ParallelNode
	etcdCluster := fmt.Sprintf("http://127.0.0.1:%d", etcdPort)
	etcdRunner = etcdstorerunner.NewETCDClusterRunner(etcdPort, 1, nil)

	if useSQL {
		mySQLRunner = mysqlrunner.NewMySQLRunner(fmt.Sprintf("diego_%d", GinkgoParallelNode()))
		mySQLProcess = ginkgomon.Invoke(mySQLRunner)
	}

	consulRunner = consulrunner.NewClusterRunner(
		9001+config.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
		1,
		"http",
	)

	logger = lagertest.NewTestLogger("test")

	bbsPort := 13000 + GinkgoParallelNode()*2
	healthPort := bbsPort + 1
	bbsAddress := fmt.Sprintf("127.0.0.1:%d", bbsPort)
	healthAddress := fmt.Sprintf("127.0.0.1:%d", healthPort)

	bbsURL := &url.URL{
		Scheme: "http",
		Host:   bbsAddress,
	}

	bbsArgs = bbsrunner.Args{
		Address:           bbsAddress,
		AdvertiseURL:      bbsURL.String(),
		AuctioneerAddress: "some-address",
		EtcdCluster:       etcdCluster,
		ConsulCluster:     consulRunner.ConsulCluster(),
		HealthAddress:     healthAddress,

		EncryptionKeys: []string{"label:key"},
		ActiveKeyLabel: "label",
	}

	if useSQL {
		bbsArgs.DatabaseDriver = "mysql"
		bbsArgs.DatabaseConnectionString = mySQLRunner.ConnectionString()
	}

	convergerConfig = &convergerrunner.Config{
		BinPath:                     binPaths.Converger,
		ConvergeRepeatInterval:      convergeRepeatInterval.String(),
		KickTaskDuration:            taskKickInterval.String(),
		ExpirePendingTaskDuration:   expirePendingTaskDuration.String(),
		ExpireCompletedTaskDuration: expireCompletedTaskDuration.String(),
		ConsulCluster:               consulRunner.ConsulCluster(),
		LogLevel:                    "info",
		BBSAddress:                  bbsURL.String(),
	}
})

var _ = SynchronizedAfterSuite(func() {
	ginkgomon.Kill(mySQLProcess)
}, func() {
	CleanupBuildArtifacts()
})

var _ = AfterEach(func() {
	if useSQL {
		mySQLRunner.Reset()
	}
})
