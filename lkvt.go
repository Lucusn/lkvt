package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"niovakv/clientapi"
	"niovakv/niovakvlib"

	"github.com/aybabtme/uniplot/histogram"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
)

type keyValue struct {
	key        bytes.Buffer
	value      bytes.Buffer
	valForPut  []byte
	keySize    int
	keyPre     []byte
	valueSize  int
	randVal    [4]byte
	crcCheck   bool
	etcdClient *clientv3.Client
	nkvcClient *clientapi.NiovakvClient
	footer     kvFooter
	count      int
	opType     int
}

type kvFooter struct {
	crc      uint32
	valSz    uint32
	timeUnix int64
	footID   byte
}

type config struct {
	putPercentage    *float64
	valueSize        *int
	keySize          *int
	keyPrefix        *string
	seed             *int64
	concurrency      *int
	endpoints        *string
	database         *int
	rSeed            *rand.Rand
	etcdClient       *clientv3.Client
	nkvcClient       clientapi.NiovakvClient
	nkvcStop         chan int
	putTimes         []time.Duration
	getTimes         []time.Duration
	wg               sync.WaitGroup
	addr             string
	port             string
	lastCon          int
	completedRequest *int64
	configPath       *string
	jsonPath         *string

	Amount     *int `json:"Request_count"`
	Putcount   int  `json:"Put_count"`
	Getcount   int  `json:"Get_count"`
	PutSuccess int  `json:"Put_success"`
	GetSuccess int  `json:"Get_success"`
	PutFailure int  `json:"Put_failures"`
	GetFailure int  `json:"Get_failures"`
}

func (conf *config) printProgress() {
	fmt.Println(" ")
	for atomic.LoadInt64(conf.completedRequest) != int64(*conf.Amount) {
		fmt.Print("\033[G\033[K")
		fmt.Print("\033[A")
		fmt.Println(atomic.LoadInt64(conf.completedRequest), " / ", int64(*conf.Amount), "request completed")
		time.Sleep(1 * time.Second)
	}
	fmt.Print("\033[G\033[K")
	fmt.Print("\033[A")
	fmt.Println(atomic.LoadInt64(conf.completedRequest), " / ", int64(*conf.Amount), "request completed")
}

func (conf *config) setUp() {
	flag.Parse()
	conf.lastCon = *conf.concurrency - 1
	endpts := strings.Split(*conf.endpoints, ",")

	addrandport := strings.Split(endpts[0], "/")
	addrport := strings.Split(addrandport[2], ":")
	conf.addr = addrport[0]
	conf.port = addrport[1]

	//random number generator for seed
	conf.rSeed = rand.New(rand.NewSource(*conf.seed))

	//random size if size= 0
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	if *conf.keySize == 0 {
		*conf.keySize = random.Intn(255-1) + 1
		log.Info("this is the random key size", *conf.keySize)
	}
	if *conf.valueSize == 0 {
		*conf.valueSize = random.Intn(1048576-16) + 16
		log.Info("this is the random value size", *conf.valueSize)
	} else if *conf.valueSize < 16 {
		*conf.valueSize = 16
	}
	count := int64(0)
	conf.completedRequest = &count

	conf.wg.Add(*conf.concurrency)
	// var err error
	conf.createclient(endpts)
	// if err != nil {
	// 	log.Fatal("could not make connection", err)
	// }
}

func (conf *config) exitApp() {
	go conf.printProgress()
	conf.wg.Wait()
	conf.stopClient()
	var floatPut = make([]float64, len(conf.putTimes))
	for i := 0; i < len(floatPut); i++ {
		floatPut[i] = float64(conf.putTimes[i].Milliseconds())
	}
	var floatGet = make([]float64, len(conf.getTimes))
	for i := 0; i < len(floatGet); i++ {
		floatGet[i] = float64(conf.getTimes[i].Milliseconds())
	}
	hPut := histogram.Hist(9, floatPut)
	hGet := histogram.Hist(9, floatGet)

	log.WithFields(log.Fields{
		"\noperations completed": *conf.Amount,
		"\nseconds to complete":  (float64(sumTime(conf.putTimes).Seconds()) / float64(*conf.concurrency)) + (float64(sumTime(conf.getTimes).Seconds()) / float64(*conf.concurrency)),
		"\ntime for puts":        float64(sumTime(conf.putTimes).Seconds()) / float64(*conf.concurrency),
		"\ntime for gets":        float64(sumTime(conf.getTimes).Seconds()) / float64(*conf.concurrency),
		"\nput per sec":          (*conf.putPercentage * float64(*conf.Amount)) / (float64(sumTime(conf.putTimes).Seconds()) / float64(*conf.concurrency)),
		"\nget per sec":          (((*conf.putPercentage - 1) * -1) * float64(*conf.Amount)) / (float64(sumTime(conf.getTimes).Seconds()) / float64(*conf.concurrency)),
		"\naverage ms per put":   (float64(sumTime(conf.putTimes).Milliseconds()) / float64(*conf.Amount)),
		"\naverage ms per get":   (float64(sumTime(conf.getTimes).Milliseconds()) / float64(*conf.Amount)),
	}).Info("done")
	log.Info("ms latency for puts")
	histogram.Fprint(os.Stdout, hPut, histogram.Linear(5))
	log.Info("ms latency for gets")
	histogram.Fprint(os.Stdout, hGet, histogram.Linear(5))
}

func (conf *config) stopClient() {
	switch *conf.database {
	case 0:
		conf.nkvcStop <- 1
	case 1:
		conf.etcdClient.Close()
	}
}

func sumTime(array []time.Duration) time.Duration {
	result := 0 * time.Second
	for _, v := range array {
		result += v
	}
	return result
}

func (o *keyValue) createKV() {
	o.createKey()
	if o.opType == 0 {
		o.createValue()
	}
}

func (o *keyValue) createKey() {
	o.key.Write(o.keyPre)
	o.key.WriteByte(byte('.'))
	countAsByte := []byte(strconv.Itoa(o.count))
	for o.key.Len() < (o.keySize - len(countAsByte)) {
		o.key.WriteByte(byte('0'))
	}
	o.key.Write(countAsByte)
	if o.key.Len() != o.keySize {
		log.Error("generated key isn't correct size. key: ", o.key.String(), " size in bytes: ", o.key.Len())
	}
}

func (o *keyValue) createValue() {
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

func (conf *config) createclient(endpoint []string) {
	var err error

	switch *conf.database {
	case 0:
		conf.nkvcStop = make(chan int)
		conf.nkvcClient.Timeout = 10 * time.Second
		go conf.nkvcClient.Start(conf.nkvcStop, *conf.configPath)
		time.Sleep(5 * time.Second)
	case 1:
		conf.etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:   endpoint,
			DialTimeout: 5 * time.Second,
		})
	}

	if err != nil {
		log.Fatal("could not make connection", err)
	}
}

func (o *keyValue) etcdPut() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	o.etcdClient.Put(ctx, o.key.String(), string(o.valForPut))
	log.WithFields(log.Fields{
		"kv.key":                o.key,
		"put value":             o.valForPut,
		"time of put Unix nano": o.footer.timeUnix,
	}).Debug("put")
	cancel()
}

func (o *keyValue) niovaPut(addr string, port string) bool {
	reqObj := niovakvlib.NiovaKV{
		InputKey:   o.key.String(),
		InputValue: o.valForPut,
	}
	//o.nkvcClient.ReqObj = &reqObj

	putStatus, _ := o.nkvcClient.Put(&reqObj)
	log.WithFields(log.Fields{
		"kv.key":                o.key,
		"put value":             o.valForPut,
		"time of put Unix nano": o.footer.timeUnix,
		"put status":            putStatus,
	}).Debug("put")
	if putStatus != 0 {
		return false
	}
	return true
}

func (o *keyValue) etcdGet() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	gr, _ := o.etcdClient.Get(ctx, o.key.String())
	cancel()
	getVal := gr.Kvs[0].Value
	log.WithFields(log.Fields{
		"get key":               gr.Kvs[0].Key,
		"get value":             getVal,
		"time of get Unix nano": o.footer.timeUnix,
	}).Debug("get")
	if len(getVal) > 0 {
		getFooter := getFooter(getVal)
		magCheck := o.magicChecker(getFooter)
		if magCheck {
			o.crcChecker(getVal, getFooter)
		} else {
			log.Error("magic check failes. ", getFooter[0], byte(175))
		}
	} else {
		log.Error("attempted get with key: ", o.key.String(), " get returned empty: ", getVal)
	}

}

func (o *keyValue) niovaGet(addr string, port string) bool {
	status := true
	reqObj := niovakvlib.NiovaKV{
		InputKey: o.key.String(),
	}
	//o.nkvcClient.ReqObj = &reqObj

	_, getVal := o.nkvcClient.Get(&reqObj)
	log.WithFields(log.Fields{
		"get key":               o.key.String(),
		"get value":             getVal,
		"time of get Unix nano": o.footer.timeUnix,
	}).Debug("get")
	if len(getVal) > 0 {
		getFooter := getFooter(getVal)
		status = o.magicChecker(getFooter)
		if status {
			o.crcChecker(getVal, getFooter)
			status = o.crcCheck
		} else {
			log.Error("magic check failes. ", getFooter[0], byte(175))
		}
	} else {
		log.Error("attempted get with key: ", o.key.String(), " get returned empty: ", getVal)
		status = false
	}
	return status
}

func getFooter(getVal []byte) [13]byte {
	var getFooter [13]byte
	for i := 0; i < 13; i++ {
		getFooter[i] = getVal[len(getVal)-13+i]
	}
	return getFooter
}

func (o *keyValue) applyFooter() {
	o.footer.footID = byte(175)
	o.valForPut = append(o.value.Bytes(), o.footer.footID) // magic number added to value
	crc := crc32.ChecksumIEEE(o.value.Bytes())
	crcArr := toByteArray(crc)
	o.valForPut = append(o.valForPut, crcArr[:]...) //crc added to value
	o.footer.crc = crc
	o.footer.timeUnix = time.Now().UnixNano()
	time := toBigByteArray(uint64(o.footer.timeUnix))
	o.valForPut = append(o.valForPut, time[:]...) //time of creation added to value
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
		log.Error("the crc check was ", o.crcCheck, getCrc, checkArr)
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

func (conf *config) execute(c int, ran []uint32, wg *sync.WaitGroup) {
	n := conf.setn(c)
	for i := 0; i < (n); i++ {
		kv := keyValue{
			keySize:    *conf.keySize,
			keyPre:     []byte(*conf.keyPrefix),
			valueSize:  *conf.valueSize,
			etcdClient: conf.etcdClient,
			nkvcClient: &conf.nkvcClient,
			randVal:    toByteArray(ran[i]),
			count:      int((*conf.Amount / *conf.concurrency)*c + i + 1),
		}

		if float64(kv.count) <= float64(*conf.Amount)*(*conf.putPercentage) {
			kv.opType = 0
		} else {
			kv.opType = 1
		}
		kv.createKV()
		conf.executeOp(kv)
		atomic.AddInt64(conf.completedRequest, int64(1))
	}
	defer wg.Done()
}

func (conf *config) executeOp(kv keyValue) {
	timer := time.Now()
	switch kv.opType {
	case 0:
		conf.lkvtPut(kv)
		stopPutTime := time.Since(timer)
		conf.putTimes = append(conf.putTimes, stopPutTime)
	case 1:
		conf.lkvtGet(kv)
		stopGetTime := time.Since(timer)
		conf.getTimes = append(conf.getTimes, stopGetTime)
	}
}

func (conf *config) lkvtPut(kv keyValue) {
	switch *conf.database {
	case 0:
		conf.Putcount += 1
		status := kv.niovaPut(conf.addr, conf.port)
		if status {
			conf.PutSuccess += 1
		} else {
			conf.PutFailure += 1
		}
	case 1:
		kv.etcdPut()
	}
}

func (conf *config) lkvtGet(kv keyValue) {
	switch *conf.database {
	case 0:
		conf.Getcount += 1
		status := kv.niovaGet(conf.addr, conf.port)
		if status {
			conf.GetSuccess += 1
		} else {
			conf.GetFailure += 1
		}
	case 1:
		kv.etcdGet()
	}
}

func (conf *config) randSetUp(c int, rSeed *rand.Rand) []uint32 {
	n := conf.setn(c)
	var ran = make([]uint32, n)
	for i := 0; i < (n); i++ {
		ran[i] = rSeed.Uint32()
	}
	return ran
}

func (conf *config) setn(c int) int {
	n := (*conf.Amount / *conf.concurrency)
	if c == conf.lastCon {
		n = (*conf.Amount / *conf.concurrency) + (*conf.Amount % *conf.concurrency)
	}
	return n
}

func main() {
	log.Info("starting the app...")
	conf := config{
		putPercentage: flag.Float64("pp", 0.50, "percentage of puts versus gets. 0.50 means 50% put 50% get"),
		valueSize:     flag.Int("vs", 0, "size of the value in bytes. min:16 bytes. ‘0’ means that the size is random"),
		keySize:       flag.Int("ks", 0, "size of the key in bytes. min:1 byte. ‘0’ means that the size is random"),
		Amount:        flag.Int("n", 1, "number of operations"),
		keyPrefix:     flag.String("kp", "key", "specify a key prefix"),
		seed:          flag.Int64("s", time.Now().UnixNano(), "seed to the random number generator"),
		concurrency:   flag.Int("c", 1, "The number of concurrent requests which may be outstanding at any one time"),
		endpoints:     flag.String("ep", "http://127.0.0.100:2380,http://127.0.0.101:2380,http://127.0.0.102:2380,http://127.0.0.103:2380,http://127.0.0.104:2380", "endpoints seperated by comas ex.http://127.0.0.100:2380,http://127.0.0.101:2380"),
		database:      flag.Int("d", 0, "the database you would like to use (0 = pmdb 1 = etcd)"),
		configPath:    flag.String("cp", "../config", "Path to niova config file"),
		jsonPath:      flag.String("jp", "execution-summary", "Path to execution summary json file"),
	}
	conf.setUp()
	for c := 0; c < *conf.concurrency; c++ {
		ran := conf.randSetUp(c, conf.rSeed)
		go conf.execute(c, ran, &conf.wg)
		time.Sleep(1000)
	}
	conf.exitApp()
	file, _ := json.MarshalIndent(conf, "", " ")
	_ = ioutil.WriteFile(*conf.jsonPath+".json", file, 0644)
}
