package modusdb

type Config struct {
	dataDir string
}

func NewDefaultConfig() Config {
	return Config{}
}

func (cc Config) WithDataDir(dir string) Config {
	cc.dataDir = dir
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
