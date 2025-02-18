/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

type Config struct {
	dataDir string

	// optional params
	limitNormalizeNode int
}

func NewDefaultConfig(dir string) Config {
	return Config{dataDir: dir, limitNormalizeNode: 10000}
}

// WithLimitNormalizeNode sets the limit for the number of nodes to normalize
// (flatten) when the @normalize directive is used in a DQL query
func (cc Config) WithLimitNormalizeNode(d int) Config {
	cc.limitNormalizeNode = d
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
