/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"github.com/go-logr/logr"
)

type Config struct {
	dataDir            string
	cacheSizeMB        int
	limitNormalizeNode int

	// logger is used for structured logging
	logger logr.Logger
}

func NewDefaultConfig(dir string) Config {
	return Config{
		dataDir:            dir,
		limitNormalizeNode: 10000,
		logger:             logr.Discard(),
		cacheSizeMB:        64, // 64 MB
	}
}

// WithLimitNormalizeNode sets the limit for the number of nodes to normalize
func (cc Config) WithLimitNormalizeNode(d int) Config {
	cc.limitNormalizeNode = d
	return cc
}

// WithLogger sets a structured logger for the engine
func (cc Config) WithLogger(logger logr.Logger) Config {
	cc.logger = logger
	return cc
}

// WithCacheSizeMB sets the memory cache size in MB
func (cc Config) WithCacheSizeMB(size int) Config {
	cc.cacheSizeMB = size
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	if cc.cacheSizeMB < 0 {
		return ErrInvalidCacheSize
	}

	return nil
}
