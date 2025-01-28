package main

import (
	"context"
	"errors"
	"flag"
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

var configFlag = flag.String("c", "config.yaml", "help message for flag n")

func main() {
	flag.Parse()

	cfg, cgfFileExists, err := config.Load(*configFlag)
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

	if cgfFileExists {
		logger.Info("using config file", zap.String("file", *configFlag))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	dbConnString, err := cfg.DB.BuildConnectionString()
	if err != nil {
		logger.Fatal("unable to build db connection string", zap.Error(err))
	}
	db, err := sqlx.Connect(cfg.DB.DriverName, dbConnString)
	if err != nil {
		logger.Fatal("unable to connect to db", zap.Error(err),
			zap.String("host", cfg.DB.Host), zap.Any("TLS", cfg.DB.TLS))
	}
	defer db.Close()

	logger.Info("connected to db")

	opts, err := cfg.Redis.ClientOptions()
	if err != nil {
		logger.Fatal("unable to build redis url", zap.Error(err))
	}

	client := redis.NewClient(opts)
	redisStatus := client.Ping(ctx)
	if redisStatus.Err() != nil {
		logger.Fatal("unable to connect to redis", zap.Error(redisStatus.Err()), zap.String("host", cfg.Redis.Host))
	}

	logger.Info("connected to redis")

	r := runner.NewRunner(cfg, db, client, logger)
	err = r.Run(ctx)
	logger.Info("runner shutting down")
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("runner ended in error", zap.Error(err))
	}
}
