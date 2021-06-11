package main

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestCrc(t *testing.T) {

	kv := keyValue{
		valueSize: 10000,
	}
	kv.randVal = toByteArray(uint32(0)) // randval determines what the value is filled with
	kv.createValue()
	kv.applyCrc()
	var crc [4]byte
	for i := 0; i < 4; i++ {
		crc[i] = kv.valForPut[10000+i]
	}

	// makes sure each bit is 0
	for i := 0; i < kv.valueSize; i++ {
		if kv.valForPut[i] != 0 { // make sure this value is the same as the argument above
			t.Errorf("value was not 0. value was %d", kv.valForPut[i])
		}
	}

	// makes sure the size is corrects
	if len(kv.valForPut) != (kv.valueSize + 4) {
		t.Errorf("value not the correct size %d", kv.value.Len())
	}

	var fakeGet bytes.Buffer
	for fakeGet.Len() < kv.valueSize {
		for i := 0; i < 4; i++ {
			fakeGet.WriteByte(kv.randVal[i])
		}
	}
	fakeGet.Truncate(kv.valueSize)

	crc_on_get := crc32.ChecksumIEEE(fakeGet.Bytes())
	getcrcArr := toByteArray(crc_on_get)
	get := append(fakeGet.Bytes(), getcrcArr[:]...)

	// compares the crc
	if getcrcArr != crc {
		t.Errorf("crc do not match")
	}
	// compares the value with the crc on the end of each
	if string(get) != string(kv.valForPut) {
		t.Errorf("value changed during the get")
	}
}
