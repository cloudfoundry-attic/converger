package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConverger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Converger Suite")
}
