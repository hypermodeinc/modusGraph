/*
 * This example is part of the ModusDB project, licensed under the Apache License 2.0.
 * You may modify and use this example in accordance with the license.
 * See the LICENSE file that accompanied this code for further details.
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

func (cc Config) WithLimitNormalizeNode(n int) Config {
	cc.limitNormalizeNode = n
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
