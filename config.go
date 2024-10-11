package modusdb

type Config struct {
	dataDir string

	// optional params
	limitNormalizeNode int
}

func NewDefaultConfig() Config {
	return Config{limitNormalizeNode: 10000}
}

func (cc Config) WithDataDir(dir string) Config {
	cc.dataDir = dir
	return cc
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
