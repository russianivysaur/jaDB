package concurrency

type Manager struct {
}

func NewConcurrencyManager() (*Manager, error) {
	return &Manager{}, nil
}
