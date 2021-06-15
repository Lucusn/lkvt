package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"hash/crc32"
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
	footID   byte
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
	rSeed         *rand.Rand
	client        clientv3.Client
	timer         time.Time
	wg            sync.WaitGroup
	// kv            keyValue
}

func (conf *config) setUp() {
	flag.Parse()
	endpts := strings.Split(*conf.endpoints, ",")

	//random number generator for seed
	conf.rSeed = rand.New(rand.NewSource(*conf.seed))

	//random size if size= 0
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	if *conf.keySize == 0 {
		*conf.keySize = random.Intn(255-1) + 1
		//what should the max value be?
		log.Info("this is the random key size", *conf.keySize)
	}
	if *conf.valueSize == 0 {
		*conf.valueSize = random.Intn(1048576-16) + 16
		//what should the max value be?
		log.Info("this is the random value size", *conf.valueSize)
	} else if *conf.valueSize < 16 {
		*conf.valueSize = 16
	}

	conf.wg.Add(*conf.concurrency)
	var err error
	conf.client, err = createclient(endpts)
	if err != nil {
		log.Fatal("could not make connection", err)
	}
}

func (conf *config) exitApp() {
	conf.wg.Wait()
	stopTime := time.Since(conf.timer)
	conf.client.Close()
	log.WithFields(log.Fields{
		"operations completed":  *conf.amount,
		"time":                  stopTime,
		"operations per second": float64(*conf.amount) / stopTime.Seconds(),
		"put per second":        (*conf.putPercentage * float64(*conf.amount)) / stopTime.Seconds(),
		"get per second":        ((((*conf.putPercentage - 1) * -1) * float64(*conf.amount)) / stopTime.Seconds()),
	}).Info("done")
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
	o.applyFooter()
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
	log.WithFields(log.Fields{
		"kv.key":                o.key,
		"put value":             string(o.valForPut),
		"time of put Unix nano": o.footer.timeUnix,
	}).Debug("put")
	cancel()
}

func (o *keyValue) sendClientGet() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gr, _ := o.client.Get(ctx, o.key.String())
	cancel()
	getVal := gr.Kvs[0].Value
	log.WithFields(log.Fields{
		"get key":               gr.Kvs[0].Key,
		"get value":             getVal,
		"time of get Unix nano": o.footer.timeUnix,
	}).Debug("get")
	getFooter := getFooter(getVal)
	magCheck := o.magicChecker(getFooter)
	if magCheck {
		o.crcChecker(getVal, getFooter)
	} else {
		log.Fatal("magic check failes. ", getFooter[0], byte(175))
	}

}

func getFooter(getVal []byte) [13]byte {
	var getFooter [13]byte
	for i := 0; i < 13; i++ {
		getFooter[i] = getVal[len(getVal)-13+i]
	}
	return getFooter
}

func (o *keyValue) applyFooter() {
	o.footer.footID = byte(175) // magic number
	o.valForPut = append(o.value.Bytes(), o.footer.footID)
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	crcArr := toByteArray(crc)
	o.valForPut = append(o.valForPut, crcArr[:]...)
	o.footer.crc = crc
}

func (o *keyValue) magicChecker(getFooter [13]byte) bool {
	var magCheck bool
	if getFooter[0] == byte(175) {
		magCheck = true
	} else {
		magCheck = false
	}
	return magCheck
}

func (o *keyValue) crcChecker(value []byte, getFooter [13]byte) {
	//checks to make sure the crc correct
	var getCrc [4]byte
	for i := 0; i < 4; i++ {
		getCrc[i] = getFooter[i+1]
	}
	val := value[0 : len(value)-len(getFooter)]
	check := crc32.ChecksumIEEE(val)
	checkArr := toByteArray(check)
	if checkArr == getCrc {
		o.crcCheck = true
	}
	if !o.crcCheck {
		log.Fatal("the crc check was ", o.crcCheck, getCrc, checkArr)
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

func (conf config) execute(c int, ran []uint32, wg *sync.WaitGroup) {
	// if wg is not sent into the method like this it doesnt work
	for i := 0; i < (*conf.amount / *conf.concurrency); i++ {
		kv := keyValue{
			keySize:   *conf.keySize,
			keyPre:    []byte(*conf.keyPrefix),
			valueSize: *conf.valueSize,
			client:    conf.client,
			randVal:   toByteArray(ran[i]),
			count:     uint32((*conf.amount / *conf.concurrency)*c + i),
		}
		if float64(i) < float64(*conf.amount / *conf.concurrency)*(*conf.putPercentage) {
			kv.createKV(0)
			kv.sendClientPut()
		} else {
			kv.createKV(1)
			kv.sendClientGet()
		}
	}
	defer wg.Done()
}

func (conf config) remainder() {
	for i := 0; i < (*conf.amount % *conf.concurrency); i++ {
		kv := keyValue{
			keySize:   *conf.keySize,
			keyPre:    []byte(*conf.keyPrefix),
			valueSize: *conf.valueSize,
			client:    conf.client,
			randVal:   toByteArray(conf.rSeed.Uint32()),
			count:     uint32((*conf.amount / *conf.concurrency)*(*conf.concurrency) + i),
		}
		if float64(i) < float64(*conf.amount%*conf.concurrency)*(*conf.putPercentage) {
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
	log.Info("starting the app...")
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
	conf.setUp()
	for c := 0; c < *conf.concurrency; c++ {
		ran := conf.randSetUp(conf.rSeed)
		conf.timer = time.Now()
		go conf.execute(c, ran, &conf.wg)
		time.Sleep(1000)
	}
	if (*conf.amount % *conf.concurrency) != 0 {
		conf.remainder()
	}
	conf.exitApp()
}
