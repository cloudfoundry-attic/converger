package lrpreprocessor_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLrpreprocessor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LRPreProcessor Suite")
}
