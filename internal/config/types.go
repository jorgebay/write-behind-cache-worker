package config

type Config struct {
	RedisUrl string   `yaml:"redisUrl" env:"WORKER_REDIS_URL" env-default:"redis://localhost:6379"`
	Db       DbConfig `yaml:"db" env-prefix:"WORKER_DB_"`
}

type DbConfig struct {
	ConnectionString string `yaml:"connectionString" env:"CONNECTION_STRING" env-default:"host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"`
	DriverName       string `yaml:"driverName" env:"DRIVER_NAME" env-default:"postgres"`
}
