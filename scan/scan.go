package scan

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt(string) (int, error)
	GetString(string) (string, error)
	GetVal(string) (any, error)
	HasField(string) bool
	Close()
}
