package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/ilyakaznacheev/cleanenv"
)

var limitRegex = regexp.MustCompile(`(?i)\bLIMIT\s+\d+`)

func Load(filename string) (*Config, bool, error) {
	var c Config
	fileExists := false
	if filename != "" {
		if _, err := os.Stat(filename); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				filename = ""
			}
		} else {
			fileExists = true
		}
	}

	var err error
	if filename == "" {
		err = cleanenv.ReadEnv(&c)
	} else {
		err = cleanenv.ReadConfig(filename, &c)
	}
	if err != nil {
		return nil, false, fmt.Errorf("unable to read config: %w", err)
	}

	if err := validate(&c); err != nil {
		return nil, false, fmt.Errorf("config is not valid: %w", err)
	}

	return &c, fileExists, nil
}

func validate(c *Config) error {
	if limitRegex.MatchString(c.Db.SelectQuery) {
		return errors.New("select query should not contain LIMIT")
	}

	if c.BatchSize <= 0 {
		return errors.New("batch size should be greater than 0")
	}

	c.Db.SelectQuery += fmt.Sprintf(" LIMIT %d", c.BatchSize)

	return nil
}
