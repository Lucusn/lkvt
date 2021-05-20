package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"time"
	//bytes may get rid of unsafe
)

type kv struct {
	key   string
	value bytes.Buffer
}

//methods for key creation, value creation, etcd-operation (put / get)

func createKey(keyPrefix string, rand string, keySize int) string {
	key := keyPrefix + "."
	for Sizeof(key) < keySize {
		// "Sizeof(key)" is just placeholder for real code... what would work here?
		key = key + rand
	}
	return key
}

func createValue(valueSize int, rand string) bytes.Buffer {
	v := new(bytes.Buffer)
	v.Grow(valueSize)
	for Sizeof(v) < uintptr(v.Cap()) {
		// "Sizeof(v)" is just placeholder for real code... what would work here?
		//also is "uintptr(v.Cap())" correct here
		v.WriteString(rand)
		//would this be the correct way to add to the buffer? is there a beter way?
	}
	return *v
}

func createclient(putPercentage float64) {
	//create client that does put/get at the ration requested
	//need to learn how
}

func applyCrc(value bytes.Buffer) bytes.Buffer {
	//take in value and add crc to the end of it
	//need to learn how
	return value_crc
}

func main() {
	fmt.Println("starting the app...")

	//FLAGS for all the parameters
	putPercentage := flag.Float64("pp", 0.50, "percentage of puts versus gets. 0.50 means 50% put 50% get")
	valueSize := flag.Int("vs", 0, "size of the value in bytes. min:16 or 32 bytes. ‘0’ means that the size is random")
	//how do you set the min size? int128? int64 and must do twice in the loop min?
	keySize := flag.Int("ks", 0, "size of the key in bytes. min:1 byte. ‘0’ means that the size is random")
	//how do you set the min size? int8?
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
	}

	//loop to create clients
	for i := 0; i < *concurrency; i++ {
		createclient(*putPercentage)
		//should the kv be sent to the client upon creation?
	}

	//key generation and value generation
	for i := 0; i < *amount; i++ {
		currentrand := fmt.Sprint(r.Int63())
		//making the random value into a string so you can add it repeatedly to the key and value
		//is this okay? is there a better way to do this?
		key := createKey(*keyPrefix, currentrand, *keySize)
		value := createValue(*valueSize, currentrand)
		valueWithcrc := applyCrc(value)
		kv := kv{key, valueWithcrc}
		//should this kv structt be sent to client within this loop?
	}
}
