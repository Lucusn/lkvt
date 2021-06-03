package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
)

type keyValue struct {
	key       bytes.Buffer
	value     bytes.Buffer
	valCrc    []byte
	keySize   int
	keyPre    []byte
	valueSize int
	putget    float64
	randVal   byte
	crcCheck  bool
	client    clientv3.Client
}

func (o *keyValue) createKV(r int32, keyran rand.Rand) {
	o.randVal = byte(r)
	o.createKey(keyran)
	o.createValue()
	o.applyCrc()
}

func (o *keyValue) createKey(keyran rand.Rand) {
	o.key.Write(o.keyPre)
	//add count here if more need be
	o.key.WriteByte(byte('.'))
	r := keyran.Uint32()
	rArr := toByteArray(r)
	for o.key.Len() < o.keySize {
		for i := 0; i < 4; i++ {
			o.key.WriteByte(rArr[i])
		}
	}
	o.key.Truncate(o.keySize)
}

func (o *keyValue) createValue() {
	o.value.Grow(o.valueSize)
	for o.value.Len() < o.valueSize {
		o.value.WriteByte(o.randVal)
	}
	o.value.Truncate(o.valueSize)
}

func createclient() clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"http://127.0.0.100:2380",
			"http://127.0.0.101:2380",
			"http://127.0.0.102:2380",
			"http://127.0.0.103:2380",
			"http://127.0.0.104:2380"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Errorf("could not make connection")
	}
	return *cli
}

func (o *keyValue) sendClientPut() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	fmt.Println("kv.key:", o.key, "\nput value", string(o.valCrc), binary.Size(o.valCrc))
	o.client.Put(ctx, o.key.String(), string(o.valCrc))
	cancel()
}

func (o *keyValue) sendClientGet() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gr, _ := o.client.Get(ctx, o.key.String())
	fmt.Println("get key: ", gr.Kvs[0].Key, "\nget value:", gr.Kvs[0].Value)
	cancel()
}

func (o *keyValue) applyCrc() {
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	crcArr := toByteArray(crc)
	o.valCrc = append(o.value.Bytes(), crcArr[:]...)
	o.crcChecker(crc)
}

func (o *keyValue) crcChecker(crc uint32) {
	//checks to make sure the crc is working correctly
	check := crc32.ChecksumIEEE(o.value.Next(o.valueSize))
	if check == crc {
		o.crcCheck = true
	}
}

func toByteArray(i uint32) (arr [4]byte) {
	binary.BigEndian.PutUint32(arr[0:4], uint32(i))
	return
}

// func test(amount int, concurrency int, keySize int, keyPrefix string, valueSize int, putPercentage float64, client clientv3.Client, r rand.Rand, wg sync.WaitGroup) { //actual function
// 	for i := 0; i < (amount / concurrency); i++ {
// 		kv := keyValue{
// 			keySize:   keySize,
// 			keyPre:    []byte(keyPrefix),
// 			valueSize: valueSize,
// 			putget:    putPercentage,
// 			client:    client,
// 		}
// 		kv.createKV(r.Int63() /*, c, i*/)
// 		fmt.Println("------------------\n", kv.key)
// 		kv.sendClient()
// 	}
// 	time.Sleep(1000) //may fix all not being sent
// 	defer wg.Done()
// }
// ----------------the wait group no longer works with this------------
// error: test passes lock by value: sync.WaitGroup contains sync.noCopycopylocks

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

	//random number generator for seed
	rSeed := rand.New(rand.NewSource(*seed))

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

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	client := createclient()
	for c := 0; c < *concurrency; c++ {
		//go test(*amount, *concurrency, *keySize, *keyPrefix, *valueSize, *putPercentage, client, *r, wg)
		go func(c int) { //if turned into actual function wait group doesnt work
			for i := 0; i < (*amount / *concurrency); i++ {
				kv := keyValue{
					keySize:   *keySize,
					keyPre:    []byte(*keyPrefix),
					valueSize: *valueSize,
					putget:    *putPercentage,
					client:    client,
				}
				//kv.createKV(rSeed.Int63())
				kv.createKV(rSeed.Int31(), *rSeed)
				fmt.Println("------------------")
				if float64(i) < float64(*amount)/float64(*concurrency)*kv.putget {
					kv.sendClientPut()
				} else {
					kv.sendClientGet()
				}
				// put/get system could be improved
				// if multiple go routines it could get discombobulated
				time.Sleep(10000)
			}

			defer wg.Done()
		}(c)
	}
	if (*amount % *concurrency) != 0 {
		for i := 0; i < (*amount % *concurrency); i++ {
			kv := keyValue{
				keySize:   *keySize,
				keyPre:    []byte(*keyPrefix),
				valueSize: *valueSize,
				putget:    *putPercentage,
				client:    client,
			}
			//kv.createKV(rSeed.Int63())
			kv.createKV(rSeed.Int31(), *rSeed)
			fmt.Println("-----------")
			if float64(i) < float64(*amount)/float64(*concurrency)*kv.putget {
				kv.sendClientPut()
			} else {
				kv.sendClientGet()
			}
			//this sorter need fixed
		}
	}
	wg.Wait()
	fmt.Println("done")
	client.Close()
	// seems not all keys are being sent to cluster
	// i belive the app is shutting down before they are sent
	// increasing sleep doesnt seem to help

	//error during run
	// 	goroutine 18 [running]:
	// main.(*keyValue).sendClientGet(0xc000577d30)
	//         /home/lucusn/etcd/etcd-custom-client/customClient.go:79 +0x290
	// main.main.func1(0xc000037978, 0xc000037988, 0xc000037970, 0xc00030cc90, 0xc000037968, 0xc000037958, 0xc000246d00, 0xc000231e00, 0xc000037990, 0x4)
	//         /home/lucusn/etcd/etcd-custom-client/customClient.go:173 +0x2d6
	// created by main.main
	//         /home/lucusn/etcd/etcd-custom-client/customClient.go:158 +0x6ec
	// exit status 2
	//
	//the following shows in the cluster
	//WARNING: 2021/06/03 16:41:27 [core] grpc: Server.processUnaryRPC failed to write status: connection error: desc = "transport is closing"
}
