package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jorgebay/write-behind-cache-worker/internal/config"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.Load("config.yml")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	db, err := sql.Open(cfg.Db.DriverName, cfg.Db.ConnectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to db")

	opts, err := redis.ParseURL(cfg.RedisUrl)
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
