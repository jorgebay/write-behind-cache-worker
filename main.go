package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/jmoiron/sqlx"
	"github.com/jorgebay/write-behind-cache-worker/internal/config"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.Load("config.yml")
	if err != nil {
		panic(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	db, err := sqlx.Connect(cfg.Db.DriverName, cfg.Db.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("Connected to db")

	opts, err := redis.ParseURL(cfg.Redis.Url)
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(opts)
	redisStatus := client.Ping(ctx)
	if redisStatus.Err() != nil {
		panic(redisStatus.Err())
	}

	fmt.Println("Connected to redis")
}
