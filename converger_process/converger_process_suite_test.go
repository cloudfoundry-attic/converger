package converger_process_test

import (
	steno "github.com/cloudfoundry/gosteno"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConverger(t *testing.T) {
	RegisterFailHandler(Fail)
	steno.EnterTestMode(steno.LOG_DEBUG)
	RunSpecs(t, "Converger Process Suite")
}
