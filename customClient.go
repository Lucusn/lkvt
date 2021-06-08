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
	startT   time.Time
	timeUnix int64
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
	//fmt.Println("kv.key:", o.key, "\nput value", string(o.valCrc), binary.Size(o.valCrc))
	o.client.Put(ctx, o.key.String(), string(o.valCrc))
	o.footer.timeUnix = int64(time.Since(o.footer.startT).Microseconds())
	cancel()
}

func (o *keyValue) sendClientGet() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gr, _ := o.client.Get(ctx, o.key.String())
	getVal := gr.Kvs[0].Value
	//fmt.Println("get key: ", gr.Kvs[0].Key, "\nget value:", getVal)
	o.footer.timeUnix = int64(time.Since(o.footer.startT).Microseconds())
	cancel()
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
	endpoints := flag.String("ep", "http://127.0.0.100:2380,http://127.0.0.101:2380,http://127.0.0.102:2380,http://127.0.0.103:2380,http://127.0.0.104:2380", "endpoints seperated by comas ex.http://127.0.0.100:2380,http://127.0.0.101:2380")
	flag.Parse()
	endpts := strings.Split(*endpoints, ",")

	//random number generator for seed
	rSeed := rand.New(rand.NewSource(*seed))

	//random number organizer to prevent keys from becoming missnamed
	// for c := 0; c < *concurrency; c++ {
	// 	for i := 0; i < (*amount / *concurrency); i++ {
	// 		arrSize := *amount / *concurrency
	// 		ranArr := [arrSize]int
	// 			:= rSeed.Uint32()
	// 	}
	// }

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
		fmt.Errorf("could not make connection")
	}
	var timePutTot int64
	var timeGetTot int64
	// const arrSize int = *amount / *concurrency
	for c := 0; c < *concurrency; c++ {
		//how do you make a variable constant? final?
		// var ranArr [arrSize]uint32
		// for i := 0; i < (*amount / *concurrency); i++ {
		// 	ranArr[i] = rSeed.Uint32()
		// }
		//go test(*amount, *concurrency, *keySize, *keyPrefix, *valueSize, *putPercentage, client, *r, wg)
		go func(c int) { //if turned into actual function wait group doesnt work
			for i := 0; i < (*amount / *concurrency); i++ {
				kv := keyValue{
					keySize:   *keySize,
					keyPre:    []byte(*keyPrefix),
					valueSize: *valueSize,
					client:    client,
					randVal:   toByteArray(rSeed.Uint32()),
					count:     uint32((*amount / *concurrency)*c + i),
				}
				kv.footer.startT = time.Now()
				kv.createKV()
				if float64(i) < float64(*amount / *concurrency)*(*putPercentage) {
					kv.sendClientPut()
					timePutTot = timePutTot + kv.footer.timeUnix
					// array filled with time per put
				} else {
					kv.sendClientGet()
					timeGetTot = timeGetTot + kv.footer.timeUnix
				}
			}
			defer wg.Done()
		}(c)
		time.Sleep(10000)
	}
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
				timePutTot = timePutTot + kv.footer.timeUnix
				// array filled with time per put
			} else {
				kv.sendClientGet()
				timeGetTot = timeGetTot + kv.footer.timeUnix
				// array filled with time per get
			}
		}
	}
	wg.Wait()
	fmt.Println("done")
	fmt.Println("time per put in microseconds", timePutTot/int64(*amount)) //change to fit ratio
	fmt.Println("time per get in microseconds", timeGetTot/int64(*amount)) //change to fit ratio

	client.Close()
	// the random number filler is not consistent with the creation
}
