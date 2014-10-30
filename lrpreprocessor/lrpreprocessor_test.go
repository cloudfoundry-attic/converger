package lrpreprocessor_test

import (
	. "github.com/cloudfoundry-incubator/converger/lrpreprocessor"
	"github.com/cloudfoundry-incubator/runtime-schema/models"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LRPreProcessor", func() {
	var (
		lrpp                *LRPreProcessor
		lrpWithPlaceholders models.DesiredLRP
		expectedLRP         models.DesiredLRP
		preProcessedLRP     models.DesiredLRP
		preProcessErr       error
	)

	BeforeEach(func() {
		lrpp = New()

		lrpWithPlaceholders = models.DesiredLRP{
			Domain:      "some-domain",
			ProcessGuid: "some-process-guid",

			Stack: "some-stack",

			Log: models.LogConfig{
				Guid:       "some-log-guid",
				SourceName: "App",
			},

			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://some-fake-file-server/some-download/path",
						To:   "/tmp/some-download",
					},
				},
				models.Parallel(
					models.ExecutorAction{
						models.RunAction{
							Path: "some-path-to-run",
						},
					},
					models.ExecutorAction{
						models.MonitorAction{
							Action: models.ExecutorAction{
								models.RunAction{
									Path: "ls",
									Args: []string{"-al"},
								},
							},
							HealthyThreshold:   1,
							UnhealthyThreshold: 1,
							HealthyHook: models.HealthRequest{
								Method: "PUT",
								URL:    "http://example.com/oh-yes/PLACEHOLDER_INSTANCE_INDEX/foo/PLACEHOLDER_INSTANCE_GUID",
							},
							UnhealthyHook: models.HealthRequest{
								Method: "PUT",
								URL:    "http://example.com/oh-no/PLACEHOLDER_INSTANCE_INDEX/foo/PLACEHOLDER_INSTANCE_GUID",
							},
						},
					},
				),
			},
		}

		expectedLRP = models.DesiredLRP{
			Domain:      "some-domain",
			ProcessGuid: "some-process-guid",

			Stack: "some-stack",

			Log: models.LogConfig{
				Guid:       "some-log-guid",
				SourceName: "App",
			},

			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://some-fake-file-server/some-download/path",
						To:   "/tmp/some-download",
					},
				},
				models.Parallel(
					models.ExecutorAction{
						models.RunAction{
							Path: "some-path-to-run",
						},
					},
					models.ExecutorAction{
						models.MonitorAction{
							Action: models.ExecutorAction{
								models.RunAction{
									Path: "ls",
									Args: []string{"-al"},
								},
							},
							HealthyThreshold:   1,
							UnhealthyThreshold: 1,
							HealthyHook: models.HealthRequest{
								Method: "PUT",
								URL:    "http://example.com/oh-yes/2/foo/some-instance-guid",
							},
							UnhealthyHook: models.HealthRequest{
								Method: "PUT",
								URL:    "http://example.com/oh-no/2/foo/some-instance-guid",
							},
						},
					},
				),
			},
		}
	})

	JustBeforeEach(func() {
		preProcessedLRP, preProcessErr = lrpp.PreProcess(lrpWithPlaceholders, 2, "some-instance-guid")
	})

	It("replaces all placeholders with their actual values", func() {
		Ω(preProcessedLRP.Actions).Should(Equal(expectedLRP.Actions))
	})

	It("does not return an error", func() {
		Ω(preProcessErr).ShouldNot(HaveOccurred())
	})
})
