/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"github.com/go-logr/logr"
)

type Config struct {
	dataDir string

	// optional params
	limitNormalizeNode int

	// logger is used for structured logging
	logger logr.Logger
}

func NewDefaultConfig(dir string) Config {
	return Config{dataDir: dir, limitNormalizeNode: 10000, logger: logr.Discard()}
}

func (cc Config) WithLimitNormalizeNode(d int) Config {
	cc.limitNormalizeNode = d
	return cc
}

// WithLogger sets a structured logger for the engine
func (cc Config) WithLogger(logger logr.Logger) Config {
	cc.logger = logger
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
