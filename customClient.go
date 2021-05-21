package main

import (
	"bytes"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"strings"
	"time"
)

type keyValue struct {
	key       string
	value     bytes.Buffer
	keySize   int
	keyPre    string
	valueSize int
	putget    float64
	randVal   string
	//*etcdClient	etcdClientclass //just place holder until I know what to put here
}

//methods for key creation, value creation, etcd-operation (put / get)

func (o *keyValue) createKey() {
	o.key = o.keyPre + "."
	var b strings.Builder
	b.WriteString(o.key)
	for b.Len() < o.keySize {
		fmt.Fprintf(&b, o.randVal)
	}
	o.key = b.String()
	o.key = firstN(o.key, o.keySize)
}

func firstN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (o *keyValue) createValue() {
	v := new(bytes.Buffer)
	v.Grow(o.valueSize)
	for v.Len() < o.valueSize {
		v.WriteString(o.randVal)
	}
	v.Truncate(o.valueSize)
	o.value = *v
}

func createclient(putPercentage float64) {
	//create client that does put/get at the ration requested
	//need to learn how
}

func (o *keyValue) applyCrc() {
	//I do not know if this is correct. definetly needs checked
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	o.value.WriteByte(byte(crc))
}

func main() {
	fmt.Println("starting the app...")

	//FLAGS for all the parameters
	putPercentage := flag.Float64("pp", 0.50, "percentage of puts versus gets. 0.50 means 50% put 50% get")
	valueSize := flag.Int("vs", 0, "size of the value in bytes. min:16 bytes. ‘0’ means that the size is random")
	keySize := flag.Int("ks", 0, "size of the key in bytes. min:1 byte. ‘0’ means that the size is random")
	amount := flag.Int("n", 1, "number of KVs to operate on")
	keyPrefix := flag.String("kp", "key", "specify a key prefix")
	seed := flag.Int64("s", time.Now().UnixNano(), "seed to the random number generator")
	concurrency := flag.Int("c", 1, "The number of concurrent etcd which may be outstanding at any one time")
	flag.Parse()

	//-----TEST to make sure all of the flags worked properly-----------
	fmt.Println("value size-----------", *valueSize)
	fmt.Println("key size-------------", *keySize)
	fmt.Println("put percentage-------", *putPercentage)
	fmt.Println("amount of keys-------", *amount)
	fmt.Println("key prefix-----------", *keyPrefix)
	fmt.Println("seed-----------------", *seed)
	fmt.Println("concurracny number---", *concurrency)
	//---------this test will be deleted later on-----------------------

	//random number generator for seed
	r := rand.New(rand.NewSource(*seed))

	//random size if size= 0
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	if *keySize == 0 {
		*keySize = random.Intn(32-1) + 1
		//what should the max value be?
		fmt.Println("this is the random key size", *keySize)
	}
	if *valueSize == 0 {
		*valueSize = random.Intn(1048576-16) + 16
		//what should the max value be?
		fmt.Println("this is the random value size", *valueSize)
	} else if *valueSize < 16 {
		*valueSize = 16
	}

	kv := keyValue{
		keySize:   *keySize,
		keyPre:    *keyPrefix,
		valueSize: *valueSize,
		putget:    *putPercentage,
	}
	//loop to create clients
	for i := 0; i < *concurrency; i++ {
		createclient(*putPercentage)
	}

	//key generation and value generation
	for i := 0; i < *amount; i++ {
		kv.randVal = fmt.Sprint(r.Int63())
		kv.createKey()
		kv.createValue()
		kv.applyCrc()
		//----------test to make sure methods worked properly-------------
		fmt.Println("current random value---", kv.randVal)
		fmt.Println("key name---------------", kv.key)
		fmt.Println("key value--------------", kv.value)
		fmt.Println("kv as string-----------", kv.value.String())
		//----------will delete this section later------------------------
		//send to client here?
	}
}
