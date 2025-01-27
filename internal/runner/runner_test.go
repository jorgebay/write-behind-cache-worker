package runner

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/ilyakaznacheev/cleanenv"
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

var (
	runner      *Runner
	redisClient *redis.Client
	db          *sqlx.DB
)

var _ = Describe("Runner", func() {
	Describe("Run", func() {
		ctx := context.WithValue(context.Background(), ctxKey("test-max-iterations"), 2)

		Context("with sample table", func() {
			BeforeEach(func() {
				deleteFrom("sample_table", 4)
			})

			It("should start from the default value", func() {
				redisClient.Del(ctx, runner.cfg.Redis.CursorKey)
				err := runner.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				expectRedisValues(ctx, "my-worker:1000:key", "2")
				expectRedisValues(ctx, "my-worker:2000:key", "3")
			})

			It("should continue from the cached cursor value", func() {
				// Delete the previous values and set the cursor
				clearRedisValues(ctx, "my-worker:1000:key", "my-worker:2000:key")
				redisClient.Set(ctx, runner.cfg.Redis.CursorKey, "2", 0)

				err := runner.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				expectRedisValues(ctx, "my-worker:2000:key", "3")
				expectRedisValuesNotFound(ctx, "my-worker:1000:key")
			})

			It("should add newer data", func() {
				redisClient.Set(ctx, runner.cfg.Redis.CursorKey, "3", 0)
				insert("sample_table", 6, 3000)
				insert("sample_table", 4, 2000)
				insert("sample_table", 5, 1000)

				err := runner.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				expectRedisValues(ctx, "my-worker:1000:key", "5")
				expectRedisValues(ctx, "my-worker:2000:key", "4")
				expectRedisValues(ctx, "my-worker:3000:key", "6")
			})
		})

		Context("with uuid table", func() {
			It("should read", func() {
				runner.cfg.DB.Cursor.Default = "00000000-0000-7300-8f14-e6ee9ef0c3f1"
				runner.cfg.DB.Cursor.Type = "uuid"
				runner.cfg.DB.SelectQuery = `
					SELECT MAX(id::text) as id, partition_key::text
					FROM uuid_table WHERE id > $1
					GROUP BY partition_key`
				runner.cfg.Redis.CursorKey = "my-worker:latest-uuid"
				redisClient.Del(ctx, runner.cfg.Redis.CursorKey)

				err := runner.Run(ctx)
				Expect(err).NotTo(HaveOccurred())

				expectRedisValues(ctx,
					"my-worker:8afb5e31-d8a6-4d92-b964-6ad8cc296050:key", "01926cc6-6430-7359-8ba1-02f348b55d36")
				expectRedisValues(ctx,
					"my-worker:65e6690c-80a6-4c76-95c7-2bbb686e4074:key", "01926cc4-cece-72d3-b801-abcb74b68556")
			})
		})
	})
})

func expectRedisValues(ctx context.Context, key string, expected string) {
	result := redisClient.Get(ctx, key)
	Expect(result.Err()).NotTo(HaveOccurred(), "redis error for key %s", key)
	Expect(result.Val()).To(Equal(expected), "redis value for key %s", key)
}

func expectRedisValuesNotFound(ctx context.Context, keys ...string) {
	result := redisClient.Exists(ctx, keys...)
	Expect(result.Val()).To(Equal(int64(0)))
}

func insert(table string, id, partitionKey any) {
	query := fmt.Sprintf("INSERT INTO %s (id, partition_key) VALUES ($1, $2) ON CONFLICT DO NOTHING", table)
	_, err := db.Queryx(query, id, partitionKey) //nolint:sqlclosecheck
	Expect(err).NotTo(HaveOccurred())
}

func deleteFrom(table string, id any) {
	query := fmt.Sprintf("DELETE FROM %s WHERE id >= $1", table)
	_, err := db.Queryx(query, id) //nolint:sqlclosecheck
	Expect(err).NotTo(HaveOccurred())
}

func clearRedisValues(ctx context.Context, keys ...string) {
	redisClient.Del(ctx, keys...)
}

var _ = BeforeSuite(func() {
	var cfg config.Config

	err := cleanenv.ReadEnv(&cfg)
	Expect(err).NotTo(HaveOccurred())
	cfg.DB.SelectQuery = "SELECT MAX(id) as id, partition_key FROM sample_table WHERE id > $1 GROUP BY partition_key"
	cfg.PollDelay = 0

	connString, err := cfg.DB.BuildConnectionString()
	Expect(err).NotTo(HaveOccurred())
	db, err = sqlx.Connect(cfg.DB.DriverName, connString)
	Expect(err).NotTo(HaveOccurred())

	opts, err := cfg.Redis.ClientOptions()
	Expect(err).NotTo(HaveOccurred())

	redisClient = redis.NewClient(opts)
	redisStatus := redisClient.Ping(context.Background())
	Expect(redisStatus.Err()).NotTo(HaveOccurred())

	logger, err := zap.NewDevelopment()
	Expect(err).NotTo(HaveOccurred())

	driver, err := postgres.WithInstance(db.DB, &postgres.Config{})
	Expect(err).NotTo(HaveOccurred())
	migrator, err := migrate.NewWithDatabaseInstance("file://test/migrations", "postgres", driver)
	Expect(err).NotTo(HaveOccurred())
	err = migrator.Up()
	if err != migrate.ErrNoChange {
		Expect(err).NotTo(HaveOccurred())
	}

	runner = &Runner{
		cfg:         &cfg,
		db:          db,
		redisClient: redisClient,
		logger:      logger,
	}
})
