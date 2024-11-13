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
