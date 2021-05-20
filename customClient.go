package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"time"
	"unsafe"
	//bytes may get rid of unsafe
)

func main() {
	fmt.Println("starting the app...")

	//flags for all the parameters
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

	//-----test to make sure all of the flags worked properly-----------
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
	//true random size if size= 0
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

	//key generation and value generation
	for i := 0; i < *amount; i++ {
		currentrand := r.Int()
		key := *keyPrefix + "." + fmt.Sprint(currentrand)
		// how do you make sure this meets the required size? for loop until it is correct size?
		fmt.Println(key)
		fmt.Println(unsafe.Sizeof(key))
		// unsafe size of returns size in bytes but seems to only return 16 on strings
		b := new(bytes.Buffer)
		b.Grow(*valueSize)
		for unsafe.Sizeof(b) < uintptr(b.Cap()) {
			fmt.Println(unsafe.Sizeof(b))
			b.WriteByte(byte(currentrand))
			//running into an infinite loop here
			//figure out the correct for argument
		}
		//here we will add the crc32

		//just a test to see what the size of the buffer and cap are after loop
		fmt.Println(unsafe.Sizeof(b))
		fmt.Println(b.Cap())
		//---------------will be deleted later---------------------------------

	}

	//build cliensts and send values to clients here

}
