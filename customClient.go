package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"strings"
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
	randVal   [4]byte
	crcCheck  bool
	client    clientv3.Client
	footer    kvFooter
	count     uint32
}
type kvFooter struct {
	crc      uint32
	valSz    uint32
	timeUnix int64 // We want to use the unix timestamp here (your choice to use nano or not) so that we can get an idea of what time and date the KV was uploaded (i.e. "June 7th, 18:38:56 2021")
}

func (o *keyValue) createKV() {
	o.createKey()
	o.createValue()
	o.applyCrc()
}

func (o *keyValue) createKey() {
	o.key.Write(o.keyPre)
	countArr := toByteArray(o.count)
	o.key.WriteByte(byte('.'))
	for o.key.Len() < o.keySize {
		for i := 0; i < 4; i++ {
			o.key.WriteByte(countArr[i])
		}
	}
	o.key.Truncate(o.keySize)
}

func (o *keyValue) createValue() {
	//value changes with each put due to concurency
	o.value.Grow(o.valueSize)
	for o.value.Len() < o.valueSize {
		for i := 0; i < 4; i++ {
			o.value.WriteByte(o.randVal[i])
		}
	}
	o.value.Truncate(o.valueSize)
	o.footer.valSz = uint32(o.value.Len())
}

func createclient(endpoint []string) (clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoint,
		DialTimeout: 5 * time.Second,
	})
	return *cli, err
}

func (o *keyValue) sendClientPut() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	o.client.Put(ctx, o.key.String(), string(o.valCrc))
	o.footer.timeUnix = time.Now().UnixNano()
	cancel()
	// fmt.Println("kv.key: ", o.key, "\nput value: ", string(o.valCrc), "\ntime of put Unix nano: ", o.footer.timeUnix)
}

func (o *keyValue) sendClientGet() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gr, _ := o.client.Get(ctx, o.key.String())
	o.footer.timeUnix = time.Now().UnixNano()
	cancel()
	getVal := gr.Kvs[0].Value
	// fmt.Println("get key: ", gr.Kvs[0].Key, "\nget value:", getVal, "\ntime of get Unix nano: ", o.footer.timeUnix)

	var getCrc [4]byte
	for i := 0; i < 4; i++ {
		getCrc[3-i] = getVal[len(getVal)-1-i]
	}
	o.crcChecker(getVal, getCrc)
}

func (o *keyValue) applyCrc() {
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	crcArr := toByteArray(crc)
	o.valCrc = append(o.value.Bytes(), crcArr[:]...)
	o.footer.crc = crc
}

func (o *keyValue) crcChecker(value []byte, crc [4]byte) {
	//checks to make sure the crc correct
	val := value[0 : len(value)-4]
	check := crc32.ChecksumIEEE(val)
	checkArr := toByteArray(check)
	if checkArr == crc {
		o.crcCheck = true
	}
	if !o.crcCheck {
		log.Fatal("the crc check was ", o.crcCheck, crc, checkArr)
	}
}

func toByteArray(i uint32) (arr [4]byte) {
	binary.BigEndian.PutUint32(arr[0:4], uint32(i))
	return
}

func test(amount int, concurrency int, c int, keySize int, keyPrefix string, valueSize int, putPercentage float64, client clientv3.Client, ran []uint32, wg *sync.WaitGroup) { //actual function
	for i := 0; i < (amount / concurrency); i++ {
		kv := keyValue{
			keySize:   keySize,
			keyPre:    []byte(keyPrefix),
			valueSize: valueSize,
			client:    client,
			randVal:   toByteArray(ran[i]),
			count:     uint32((amount/concurrency)*c + i),
		}
		kv.createKV()
		if float64(i) < float64(amount/concurrency)*(putPercentage) {
			kv.sendClientPut()
		} else {
			kv.sendClientGet()
		}
	}
	defer wg.Done()
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
	endpoints := flag.String("ep", "http://127.0.0.100:2380,http://127.0.0.101:2380,http://127.0.0.102:2380,http://127.0.0.103:2380,http://127.0.0.104:2380", "endpoints seperated by comas ex.http://127.0.0.100:2380,http://127.0.0.101:2380")
	flag.Parse()
	endpts := strings.Split(*endpoints, ",")

	//random number generator for seed
	rSeed := rand.New(rand.NewSource(*seed))

	//random size if size= 0
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	if *keySize == 0 {
		*keySize = random.Intn(255-1) + 1
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
	client, err := createclient(endpts)
	if err != nil {
		log.Fatal("could not make connection", err)
	}

	var timer1 time.Time
	for c := 0; c < *concurrency; c++ {
		//this organizes the values so they stay the same each generation
		var ran = make([]uint32, *amount / *concurrency)
		for i := 0; i < (*amount / *concurrency); i++ {
			ran[i] = rSeed.Uint32()
		}
		timer1 = time.Now()
		go test(*amount, *concurrency, c, *keySize, *keyPrefix, *valueSize, *putPercentage, client, ran, &wg)
		time.Sleep(1000)
	}
	// this portion ensures that any kvs missed are entered correctly
	if (*amount % *concurrency) != 0 {
		for i := 0; i < (*amount % *concurrency); i++ {
			kv := keyValue{
				keySize:   *keySize,
				keyPre:    []byte(*keyPrefix),
				valueSize: *valueSize,
				client:    client,
				randVal:   toByteArray(rSeed.Uint32()),
				count:     uint32((*amount / *concurrency)*(*concurrency) + i),
			}
			kv.createKV()
			if float64(i) < float64(*amount%*concurrency)*(*putPercentage) {
				kv.sendClientPut()
			} else {
				kv.sendClientGet()
			}
		}
	}
	wg.Wait()
	stopTime := time.Since(timer1)
	client.Close()
	fmt.Println("done")
	fmt.Printf("%v operations completed in %v\n%v operations per second \n%v put per second \n%v get per second \n", *amount, stopTime, float64(*amount)/stopTime.Seconds(), (*putPercentage*float64(*amount))/stopTime.Seconds(), (((*putPercentage-1)*-1)*float64(*amount))/stopTime.Seconds())
	//the put and get per second should be more accurate
	//this assumes that the put and get take the same time
}
