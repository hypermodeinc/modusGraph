package modusdb

type Config struct {
	// optional params
	limitNormalizeNode int
}

func NewDefaultConfig() Config {
	return Config{limitNormalizeNode: 10000}
}

func (cc Config) WithLimitNormalizeNode(n int) Config {
	cc.limitNormalizeNode = n
	return cc
}
