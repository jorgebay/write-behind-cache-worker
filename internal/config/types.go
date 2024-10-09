package config

import (
	"cmp"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"go.uber.org/zap"
)

var parameterRegex = regexp.MustCompile(`\$\{(.+?)\}`)

type Config struct {
	Redis     RedisConfig   `yaml:"redis" env-prefix:"WORKER_REDIS_"`
	Db        DbConfig      `yaml:"db" env-prefix:"WORKER_DB_"`
	PollDelay time.Duration `yaml:"pollDelay" env:"WORKER_POLL_DELAY" env-default:"2s"`
	Debug     bool          `yaml:"debug" env:"WORKER_DEBUG" env-default:"false"`
}

type DbConfig struct {
	ConnectionString string       `yaml:"connectionString" env:"CONNECTION_STRING" env-default:"host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"`
	DriverName       string       `yaml:"driverName" env:"DRIVER_NAME" env-default:"postgres"`
	SelectQuery      string       `yaml:"selectQuery" env:"SELECT_QUERY" env-default:"SELECT MAX(id) as id, partition_key FROM sample_table WHERE id > $1 GROUP BY partition_key"`
	Cursor           CursorConfig `yaml:"cursor" env-prefix:"CURSOR_"`
}

type RedisConfig struct {
	Url       string `yaml:"url" env:"URL" env-default:"redis://localhost:6379"`
	Key       string `yaml:"key" env:"KEY" env-default:"my-worker:${partition_key}:key"`
	Value     string `yaml:"value" env:"VALUE" env-default:"${id}"`
	CursorKey string `yaml:"cursorKey" env:"CURSOR_KEY" env-default:"my-worker:latest"`
}

type CursorConfig struct {
	Column  string `yaml:"column" env:"COLUMN" env-default:"id"`
	Type    string `yaml:"type" env:"TYPE" env-default:"int64"`
	Default string `yaml:"default" env:"DEFAULT" env-default:"-1"`
}

type ComparatorFunc func(a, b any) (int, error)
type KeyFunc func(row map[string]any) string
type ValueFunc func(row map[string]any) any

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

func (c *CursorConfig) Info(backendDefault string) (*CursorInfo, error) {
	defaultValueString := c.Default
	if backendDefault != "" {
		defaultValueString = backendDefault
	}
	defaultValue, compareFunc, err := toDbType(defaultValueString, c.Type)
	if err != nil {
		return nil, fmt.Errorf("unable to convert default value: %w", err)
	}
	return &CursorInfo{
		Column:      c.Column,
		Type:        c.Type,
		Default:     defaultValue,
		CompareFunc: compareFunc,
	}, nil
}

type CursorInfo struct {
	Column      string
	Type        string
	Default     any
	CompareFunc ComparatorFunc
}

func toDbType(value string, dbType string) (any, ComparatorFunc, error) {
	switch dbType {
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		return i, getCompareFunc[int64](), err
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		return int32(i), getCompareFunc[int32](), err
	case "int":
		i, err := strconv.Atoi(value)
		return i, getCompareFunc[int](), err
	case "string":
		return value, getCompareFunc[string](), nil
	case "uuid":
		return value, getCompareFunc[string](), nil
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
