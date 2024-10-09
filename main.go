package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/jmoiron/sqlx"
	"github.com/jorgebay/write-behind-cache-worker/internal/config"
	"github.com/jorgebay/write-behind-cache-worker/internal/runner"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load("config.yml")
	if err != nil {
		panic(fmt.Sprintf("unable to load config: %s", err))
	}

	var logger *zap.Logger
	if cfg.Debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	db, err := sqlx.Connect(cfg.Db.DriverName, cfg.Db.ConnectionString)
	if err != nil {
		logger.Fatal("unable to connect to db", zap.Error(err))
	}
	defer db.Close()

	logger.Info("connected to db")

	opts, err := redis.ParseURL(cfg.Redis.Url)
	if err != nil {
		logger.Fatal("unable to parse redis url", zap.Error(err))
	}

	client := redis.NewClient(opts)
	redisStatus := client.Ping(ctx)
	if redisStatus.Err() != nil {
		logger.Fatal("unable to connect to redis", zap.Error(redisStatus.Err()))
	}

	logger.Info("connected to redis")

	runner := runner.NewRunner(cfg, db, client, logger)
	err = runner.Run(ctx)
	logger.Info("runner shutting down")
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("runner ended in error", zap.Error(err))
	}
}
