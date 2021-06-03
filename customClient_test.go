package main

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestCrc(t *testing.T) {

	kv := keyValue{
		valueSize: 10000,
		randVal:   0, // randval determines what the value is filled with
	}
	kv.createValue()
	kv.applyCrc()
	var crc [4]byte
	for i := 0; i < 4; i++ {
		crc[i] = kv.valCrc[10000+i]
	}

	for i := 0; i < kv.valueSize; i++ {
		if kv.valCrc[i] != 0 { // make sure this value is the same as the argument above
			t.Errorf("value was not 0. value was %d", kv.valCrc[i])
		}

	}

	if len(kv.valCrc) != (kv.valueSize + 4) {
		t.Errorf("value not the correct size %d", kv.value.Len())
	}

	var fakeGet bytes.Buffer
	for i := 0; i < kv.valueSize; i++ {
		fakeGet.WriteByte(kv.valCrc[i])
	}

	crc_on_get := crc32.ChecksumIEEE(fakeGet.Bytes())
	getcrcArr := toByteArray(crc_on_get)
	get := append(fakeGet.Bytes(), getcrcArr[:]...)

	if getcrcArr != crc {
		t.Errorf("crc do not match")
	}
	// compares the value with the crc on the end of each
	if string(get) != string(kv.valCrc) {
		t.Errorf("value changed during the get")
	}
}
