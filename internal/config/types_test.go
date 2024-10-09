package config

import (
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

func TestRunner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}

var _ = Describe("RedisConfig", func() {
	logger, err := zap.NewDevelopment()
	Expect(err).NotTo(HaveOccurred())
	row := map[string]any{
		"id":    1,
		"hello": "world",
	}

	Describe("KeyFn()", func() {
		It("should fail if no columns are found in key", func() {
			c := RedisConfig{Key: "worker"}
			_, err := c.KeyFn(logger)
			Expect(err).To(HaveOccurred())
		})

		tests := [][]string{
			{"worker:${id}", "worker:1"},
			{"worker:${id}:latest", "worker:1:latest"},
			{"worker:${id}:hello:${hello}", "worker:1:hello:world"},
			{"worker:${id}:hello:${hello}:last", "worker:1:hello:world:last"},
		}

		for _, test := range tests {
			It(fmt.Sprintf("should return a function that parses a key for %s", test[0]), func() {
				c := RedisConfig{Key: test[0]}
				fn, err := c.KeyFn(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(fn(row)).To(Equal(test[1]))
			})
		}
	})

	Describe("ValueFn()", func() {
		tests := []struct {
			text     string
			value    any
			expected any
		}{
			{"${id}", 1, 1}, // It returns the same type
			{"${hello}", "world", "world"},
			{"worker:${id}:${hello}", 1, "worker:1:world"},
			{"worker:${id}:test:${hello}", 1, "worker:1:test:world"},
		}

		for _, test := range tests {
			It(fmt.Sprintf("should return a function that parses a value for %s", test.text), func() {
				c := RedisConfig{Value: test.text}
				fn, err := c.ValueFn(logger)
				Expect(err).NotTo(HaveOccurred())
				Expect(fn(row)).To(Equal(test.expected))
			})
		}
	})
})
