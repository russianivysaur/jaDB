package utils

import (
	"encoding/binary"
	"hash/fnv"
)

func HashCode(key any) (uint32, error) {
	hashCode := fnv.New32a()
	switch key.(type) {
	case int64:
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(key.(int64)))
		if _, err := hashCode.Write(buffer); err != nil {
			return 0, err
		}
	case string:
		if _, err := hashCode.Write([]byte(key.(string))); err != nil {
			return 0, err
		}
	}
	return hashCode.Sum32(), nil
}
