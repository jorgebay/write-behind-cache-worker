package runner

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/jorgebay/write-behind-cache-worker/internal/config"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestRunner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Runner Suite")
}

var runner *Runner

var _ = BeforeSuite(func() {
	db, err := sqlx.Connect(
		"postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	Expect(err).NotTo(HaveOccurred())

	opts, err := redis.ParseURL("redis://localhost:6379")
	Expect(err).NotTo(HaveOccurred())

	redisClient := redis.NewClient(opts)
	redisStatus := redisClient.Ping(context.Background())
	Expect(redisStatus.Err()).NotTo(HaveOccurred())

	logger, err := zap.NewDevelopment()
	Expect(err).NotTo(HaveOccurred())

	runner = &Runner{
		cfg: &config.Config{
			Db: config.DbConfig{
				SelectQuery: "SELECT MAX(id) as id, partition_id FROM sample_table WHERE id > $1 GROUP BY partition_id",
				Cursor: config.CursorConfig{
					Column:  "id",
					Type:    "int64",
					Default: "-1",
				},
			},
		},
		db:          db,
		redisClient: redisClient,
		logger:      logger,
	}
})

var _ = Describe("Runner", func() {
	Describe("Run", func() {
		It("should return nil", func() {
			ctx := context.WithValue(context.Background(), "test-max-iterations", 2)
			err := runner.Run(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
