package main

type clientWrapper interface {
	Connect(addr, username, password string) error
	Close()

	Get(id string) ([]byte, error)
	Upsert(id string, value []byte) error
}
