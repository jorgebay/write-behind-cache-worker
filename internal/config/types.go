package config

import (
	"cmp"
	"crypto/tls"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var parameterRegex = regexp.MustCompile(`\$\{(.+?)\}`)

type Config struct {
	Redis     RedisConfig   `yaml:"redis" env-prefix:"WORKER_REDIS_"`
	DB        DBConfig      `yaml:"db" env-prefix:"WORKER_DB_"`
	PollDelay time.Duration `yaml:"pollDelay" env:"WORKER_POLL_DELAY" env-default:"2s"`
	Debug     bool          `yaml:"debug" env:"WORKER_DEBUG" env-default:"false"`
	BatchSize int           `yaml:"batchSize" env:"WORKER_BATCH_SIZE" env-default:"200"`
}

type DBConfig struct {
	ConnectionString string       `yaml:"connectionString" env:"CONNECTION_STRING"`
	DriverName       string       `yaml:"driverName" env:"DRIVER_NAME" env-default:"postgres"`
	SelectQuery      string       `yaml:"selectQuery" env:"SELECT_QUERY" env-default:"SELECT MAX(id) as id, partition_key FROM sample_table WHERE id > $1 GROUP BY partition_key"` //nolint:lll
	Cursor           CursorConfig `yaml:"cursor" env-prefix:"CURSOR_"`
	Host             string       `yaml:"host" env:"HOST" env-default:"localhost"`
	Port             int          `yaml:"port" env:"PORT" env-default:"5432"`
	User             string       `yaml:"user" env:"USER" env-default:"postgres"`
	Password         string       `yaml:"password" env:"PASSWORD" env-default:"postgres"`
	DBName           string       `yaml:"dbName" env:"DBNAME" env-default:"postgres"`
	TLS              DBTLSConfig  `yaml:"tls" env:"TLS"`
}

type DBTLSConfig struct {
	Mode     string `yaml:"mode" env:"MODE" env-default:"disable"`
	RootCert string `yaml:"rootCert" env:"ROOTCERT"`
}

type RedisConfig struct {
	URL       string         `yaml:"url" env:"URL"`
	Host      string         `yaml:"host" env:"HOST" env-default:"localhost"`
	Port      int            `yaml:"port" env:"PORT" env-default:"6379"`
	User      string         `yaml:"user" env:"USER"`
	Password  string         `yaml:"password" env:"PASSWORD"`
	TLS       RedisTLSConfig `yaml:"tls" env:"TLS"`
	Key       string         `yaml:"key" env:"KEY" env-default:"my-worker:${partition_key}:key"`
	Value     string         `yaml:"value" env:"VALUE" env-default:"${id}"`
	CursorKey string         `yaml:"cursorKey" env:"CURSOR_KEY" env-default:"my-worker:latest"`
}

type RedisTLSConfig struct {
	InsecureSkipVerify bool `yaml:"insecureSkipVerify" env:"INSECURE_SKIP_VERIFY" env-default:"false"`
}

type CursorConfig struct {
	Column  string `yaml:"column" env:"COLUMN" env-default:"id"`
	Type    string `yaml:"type" env:"TYPE" env-default:"int64"`
	Default string `yaml:"default" env:"DEFAULT" env-default:"-1"`
}

type (
	ComparatorFunc func(a, b any) (int, error)
	ConvertFunc    func(value string) (any, error)
	KeyFunc        func(row map[string]any) string
	ValueFunc      func(row map[string]any) any
)

func (c *DBConfig) BuildConnectionString() (string, error) {
	if c.ConnectionString != "" {
		return c.ConnectionString, nil
	}

	if c.DriverName != "postgres" {
		return "", fmt.Errorf("unsupported driver to build the connection string: %s", c.DriverName)
	}

	result := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.TLS.Mode)

	if c.TLS.RootCert != "" {
		result += fmt.Sprintf(" sslrootcert=%s", c.TLS.RootCert)
	}

	return result, nil
}

func (c *RedisConfig) ClientOptions() (*redis.Options, error) {
	if c.URL != "" {
		return redis.ParseURL(c.URL)
	}

	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Username: c.User,
		Password: c.Password,
	}

	if c.TLS.InsecureSkipVerify {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
		}
	}

	return opts, nil
}

func (c *RedisConfig) KeyFn(logger *zap.Logger) (KeyFunc, error) {
	formatString, columnNames, err := parseColumns(c.Key)
	if err != nil {
		return nil, err
	}

	logger.Info("Using redis key", zap.String("key", formatString), zap.Any("columnNames", columnNames))

	return func(row map[string]any) string {
		args := make([]any, 0, len(columnNames))
		for _, name := range columnNames {
			value := row[name]
			args = append(args, value)
		}

		return fmt.Sprintf(formatString, args...)
	}, nil
}

func (c *RedisConfig) ValueFn(logger *zap.Logger) (ValueFunc, error) {
	formatString, columnNames, err := parseColumns(c.Value)
	if err != nil {
		return nil, err
	}

	logger.Info("Using redis value", zap.String("value", formatString), zap.Any("columnNames", columnNames))

	return func(row map[string]any) any {
		if len(columnNames) == 1 && formatString == "%v" {
			// Use the same type
			return row[columnNames[0]]
		}

		args := make([]any, 0, len(columnNames))
		for _, name := range columnNames {
			value := row[name]
			args = append(args, value)
		}

		return fmt.Sprintf(formatString, args...)
	}, nil
}

func parseColumns(text string) (string, []string, error) {
	matches := parameterRegex.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return "", nil, fmt.Errorf("no parameters found in key/value: %s", text)
	}

	formatString := ""
	index := 0
	columnNames := make([]string, 0, len(matches))
	for _, match := range matches {
		formatString += text[index:match[0]] + "%v"
		index = match[1]
		column := text[match[2]:match[3]]
		columnNames = append(columnNames, column)
	}

	if index < len(text) {
		formatString += text[index:]
	}

	return formatString, columnNames, nil
}

func (c *CursorConfig) Info() (*CursorInfo, error) {
	convertFunc, compareFunc, err := toDBTypeFuncs(c.Type)
	if err != nil {
		return nil, err
	}

	defaultValue, err := convertFunc(c.Default)
	if err != nil {
		return nil, fmt.Errorf("unable to convert default value: %w", err)
	}

	return &CursorInfo{
		Column:      c.Column,
		Type:        c.Type,
		Default:     defaultValue,
		CompareFunc: compareFunc,
		ConvertFunc: convertFunc,
	}, nil
}

type CursorInfo struct {
	Column      string
	Type        string
	Default     any
	CompareFunc ComparatorFunc
	ConvertFunc ConvertFunc
}

func toDBTypeFuncs(dbType string) (ConvertFunc, ComparatorFunc, error) {
	switch dbType {
	case "int64":
		return func(value string) (any, error) {
			return strconv.ParseInt(value, 10, 64)
		}, getCompareFunc[int64](), nil
	case "int32":
		return func(value string) (any, error) {
			return strconv.ParseInt(value, 10, 32)
		}, getCompareFunc[int64](), nil
	case "int":
		return func(value string) (any, error) {
			return strconv.Atoi(value)
		}, getCompareFunc[int](), nil
	case "string":
		return func(value string) (any, error) {
			return value, nil
		}, getCompareFunc[string](), nil
	case "uuid":
		return func(value string) (any, error) {
			return value, nil
		}, getCompareFunc[string](), nil
	}

	return nil, nil, fmt.Errorf("unsupported type: %s", dbType)
}

func getCompareFunc[T cmp.Ordered]() ComparatorFunc {
	return func(a, b any) (int, error) {
		aComparable, ok := a.(T)
		if !ok {
			return 0, fmt.Errorf("unable to convert a to %T", aComparable)
		}
		bComparable, ok := b.(T)
		if !ok {
			return 0, fmt.Errorf("unable to convert a to %T", bComparable)
		}
		return cmp.Compare(aComparable, bComparable), nil
	}
}
