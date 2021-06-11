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
	valForPut []byte
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
	timeUnix int64
}

type config struct {
	putPercentage *float64
	valueSize     *int
	keySize       *int
	amount        *int
	keyPrefix     *string
	seed          *int64
	concurrency   *int
	endpoints     *string
}

func (conf *config) setUp() (*rand.Rand, clientv3.Client, time.Time, *sync.WaitGroup) {
	flag.Parse()
	endpts := strings.Split(*conf.endpoints, ",")

	//random number generator for seed
	rSeed := rand.New(rand.NewSource(*conf.seed))

	//random size if size= 0
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	if *conf.keySize == 0 {
		*conf.keySize = random.Intn(255-1) + 1
		//what should the max value be?
		fmt.Println("this is the random key size", *conf.keySize)
	}
	if *conf.valueSize == 0 {
		*conf.valueSize = random.Intn(1048576-16) + 16
		//what should the max value be?
		fmt.Println("this is the random value size", *conf.valueSize)
	} else if *conf.valueSize < 16 {
		*conf.valueSize = 16
	}

	var wg sync.WaitGroup
	wg.Add(*conf.concurrency)
	client, err := createclient(endpts)
	if err != nil {
		log.Fatal("could not make connection", err)
	}

	var timer time.Time

	return rSeed, client, timer, &wg
}

func (conf *config) exitApp(wg *sync.WaitGroup, timer time.Time, client clientv3.Client) {
	wg.Wait()
	stopTime := time.Since(timer)
	client.Close()
	fmt.Println("done")
	fmt.Printf("%v operations completed in %v\n%v operations per second \n%v put per second \n%v get per second \n",
		*conf.amount, stopTime,
		float64(*conf.amount)/stopTime.Seconds(),
		(*conf.putPercentage*float64(*conf.amount))/stopTime.Seconds(),
		(((*conf.putPercentage-1)*-1)*float64(*conf.amount))/stopTime.Seconds())
}

func (o *keyValue) createKV(op int) {
	o.createKey()
	if op == 0 {
		o.createValue()
	}
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
	o.applyCrc()
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
	o.footer.timeUnix = time.Now().UnixNano()
	time := toBigByteArray(uint64(o.footer.timeUnix))
	o.valForPut = append(o.valForPut, time[:]...)
	o.client.Put(ctx, o.key.String(), string(o.valForPut))
	// fmt.Println("kv.key: ", o.key, "\nput value: ", string(o.valForPut), "\ntime of put Unix nano: ", o.footer.timeUnix)
	cancel()
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
		getCrc[3-i] = getVal[len(getVal)-9-i]
	}
	o.crcChecker(getVal, getCrc)
}

func (o *keyValue) applyCrc() {
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	crcArr := toByteArray(crc)
	o.valForPut = append(o.value.Bytes(), crcArr[:]...)
	o.footer.crc = crc
}

func (o *keyValue) crcChecker(value []byte, crc [4]byte) {
	//checks to make sure the crc correct
	val := value[0 : len(value)-12]
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

func toBigByteArray(i uint64) (arr [8]byte) {
	binary.BigEndian.PutUint64(arr[0:8], uint64(i))
	return
}

func test(amount int, concurrency int, c int, keySize int, keyPrefix string, valueSize int, putPercentage float64, client clientv3.Client, ran []uint32, wg *sync.WaitGroup) {
	for i := 0; i < (amount / concurrency); i++ {
		kv := keyValue{
			keySize:   keySize,
			keyPre:    []byte(keyPrefix),
			valueSize: valueSize,
			client:    client,
			randVal:   toByteArray(ran[i]),
			count:     uint32((amount/concurrency)*c + i),
		}
		if float64(i) < float64(amount/concurrency)*(putPercentage) {
			kv.createKV(0)
			kv.sendClientPut()
		} else {
			kv.createKV(1)
			kv.sendClientGet()
		}
	}
	defer wg.Done()
}

func remainder(amount int, concurrency int, keySize int, keyPrefix string, valueSize int, putPercentage float64, client clientv3.Client, rSeed rand.Rand) {
	for i := 0; i < (amount % concurrency); i++ {
		kv := keyValue{
			keySize:   keySize,
			keyPre:    []byte(keyPrefix),
			valueSize: valueSize,
			client:    client,
			randVal:   toByteArray(rSeed.Uint32()),
			count:     uint32((amount/concurrency)*(concurrency) + i),
		}
		if float64(i) < float64(amount%concurrency)*(putPercentage) {
			kv.createKV(0)
			kv.sendClientPut()
		} else {
			kv.createKV(1)
			kv.sendClientGet()
		}
	}
}

func (conf *config) randSetUp(rSeed *rand.Rand) []uint32 {
	var ran = make([]uint32, *conf.amount / *conf.concurrency)
	for i := 0; i < (*conf.amount / *conf.concurrency); i++ {
		ran[i] = rSeed.Uint32()
	}
	return ran
}

func main() {
	fmt.Println("starting the app...")
	conf := config{
		putPercentage: flag.Float64("pp", 0.50, "percentage of puts versus gets. 0.50 means 50% put 50% get"),
		valueSize:     flag.Int("vs", 0, "size of the value in bytes. min:16 bytes. ‘0’ means that the size is random"),
		keySize:       flag.Int("ks", 0, "size of the key in bytes. min:1 byte. ‘0’ means that the size is random"),
		amount:        flag.Int("n", 1, "number of KVs to operate on"),
		keyPrefix:     flag.String("kp", "key", "specify a key prefix"),
		seed:          flag.Int64("s", time.Now().UnixNano(), "seed to the random number generator"),
		concurrency:   flag.Int("c", 1, "The number of concurrent etcd which may be outstanding at any one time"),
		endpoints:     flag.String("ep", "http://127.0.0.100:2380,http://127.0.0.101:2380,http://127.0.0.102:2380,http://127.0.0.103:2380,http://127.0.0.104:2380", "endpoints seperated by comas ex.http://127.0.0.100:2380,http://127.0.0.101:2380"),
	}
	rSeed, client, timer, wg := conf.setUp()
	for c := 0; c < *conf.concurrency; c++ {
		ran := conf.randSetUp(rSeed)
		timer = time.Now()
		go test(*conf.amount, *conf.concurrency, c, *conf.keySize, *conf.keyPrefix, *conf.valueSize, *conf.putPercentage, client, ran, wg)
		time.Sleep(1000)
	}
	if (*conf.amount % *conf.concurrency) != 0 {
		remainder(*conf.amount, *conf.concurrency, *conf.keySize, *conf.keyPrefix, *conf.valueSize, *conf.putPercentage, client, *rSeed)
	}
	conf.exitApp(wg, timer, client)
}
