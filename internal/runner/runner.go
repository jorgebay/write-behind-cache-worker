package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jorgebay/write-behind-cache-worker/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Runner struct {
	cfg         *config.Config
	db          *sqlx.DB
	redisClient *redis.Client
	logger      *zap.Logger
}

func NewRunner(cfg *config.Config, db *sqlx.DB, redisClient *redis.Client, logger *zap.Logger) *Runner {
	return &Runner{
		cfg:         cfg,
		db:          db,
		redisClient: redisClient,
		logger:      logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	redisCursorDefaultValue := r.redisClient.Get(ctx, r.cfg.Redis.CursorKey)
	cursorInfo, err := r.cfg.Db.Cursor.Info(redisCursorDefaultValue.Val())
	if err != nil {
		return err
	}

	cursorValue := cursorInfo.Default
	keyFn, err := r.cfg.Redis.KeyFn(r.logger)
	if err != nil {
		return err
	}
	valueFn, err := r.cfg.Redis.ValueFn(r.logger)
	if err != nil {
		return err
	}
	compareFunc := cursorInfo.CompareFunc
	shouldStop := shouldStopFn(ctx)

	for i := 0; !shouldStop(i); i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			// continue
		}

		r.logger.Debug("running db query", zap.Any("cursorValue", cursorValue))
		rows, err := r.db.QueryxContext(ctx, r.cfg.Db.SelectQuery, cursorValue)
		if err != nil {
			r.logger.Error("unable to query db", zap.Error(err), zap.String("query", r.cfg.Db.SelectQuery))
			return err
		}

		totalRows := 0

		redisPipeline := r.redisClient.Pipeline()

		for rows.Next() {
			m := make(map[string]any)
			err := rows.MapScan(m)
			if err != nil {
				return fmt.Errorf("unable to map scan: %w", err)
			}

			currentCursorValue := m[cursorInfo.Column]
			if currentCursorValue == nil {
				return fmt.Errorf("cursor column '%s' is nil or does not exists", cursorInfo.Column)
			}

			comparison, err := compareFunc(cursorValue, currentCursorValue)
			if err != nil {
				return fmt.Errorf("unable to compare %v and %v: %w", cursorValue, currentCursorValue, err)
			}

			if comparison < 0 {
				cursorValue = currentCursorValue
			}

			key := keyFn(m)
			value := valueFn(m)
			r.logger.Debug("setting key", zap.String("key", key), zap.Any("value", value))
			redisPipeline.Set(ctx, key, value, 0)
			totalRows++
		}

		if totalRows > 0 {
			r.logger.Info("processed rows", zap.Int("rows", totalRows))
			if totalRows == r.cfg.BatchSize {
				r.logger.Warn("batch size reached, consider incrementing the execution rate",
					zap.Int("batchSize", r.cfg.BatchSize))
			}

			r.logger.Debug("setting cursor", zap.Any("cursorValue", cursorValue))
			redisPipeline.Set(ctx, r.cfg.Redis.CursorKey, fmt.Sprint(cursorValue), 0)
			redisPipeline.Exec(ctx)
		}

		rows.Close()

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.PollDelay):
			// continue
		}
	}

	return nil
}

func shouldStopFn(ctx context.Context) func(int) bool {
	maxIterations := ctx.Value("test-max-iterations")
	if maxIterations != nil {
		maxIterationsInt, ok := maxIterations.(int)
		if !ok {
			panic("invalid max iterations")
		}

		return func(i int) bool {
			return i >= maxIterationsInt
		}
	}

	// Default: no artificial stopping
	return func(_ int) bool {
		return false
	}
}
