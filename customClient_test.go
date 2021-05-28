package main

import (
	"bytes"
	"hash/crc32"
	"math/rand"
	"testing"
	"time"
)

func TestCrc(t *testing.T) {
	//quick test to make crc works properly
	for i := 0; i < 1000; i++ {
		random := rand.New(rand.NewSource(time.Now().UnixNano()))

		curRand := random.Intn(30000-1) + 1
		var value bytes.Buffer
		value.Grow(curRand)
		for value.Len() < curRand {
			value.WriteByte(byte(random.Int()))
		}
		value.Truncate(curRand)

		crc := crc32.ChecksumIEEE(value.Bytes())
		value.WriteByte(byte(crc))

		check := crc32.ChecksumIEEE(value.Next(curRand))

		if check != crc {
			t.Errorf("crcCheck check was false")
		}
	}
}
