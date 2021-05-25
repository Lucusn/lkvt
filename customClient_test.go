package main

import (
	//	"bytes"
	//	"errors"
	//	"flag"
	"fmt"
	//	"hash/crc32"
	"math/rand"
	//	"strings"
	"testing"
	"time"
	//"go.etcd.io/etcd/clientv3"//this import is generating an error
)

func TestCrc(t *testing.T) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	fmt.Printf("Hello rand=%d", random.Int())

	x := random.Int()

	if x != 0 {
		t.Errorf("x = %d; want 0", x)
	}

	t.Error()
}
