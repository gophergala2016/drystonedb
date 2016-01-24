package drystonedb

import (
	"fmt"
	//"io/ioutil"
	"log"
	//"net/http"
	_ "net/url"
	"strings"
	"sync"
	"time"
	//"strconv"
	"math/rand"
)

const (
	StoneHeartBeatTimeInterval = time.Second * 15
)

//
// terminology:
// stone = node
// wall = group of stones
// stone wall = aka cluster
//

type Drystone struct {
	CURL string // client url
	SURL string // stone url
	//WURLS         []string // wall urls list
	WURLS         map[string]*UrlStone
	wuMux         sync.Mutex
	nm            string // name
	HeartBeatQuit chan bool
	data          map[string]map[string]*DataStone // data storage
	daMux         sync.Mutex                       // one mux for all, not perfect but...
	AddUrlsLimit  int 
}

type UrlStone struct {
	ec uint32 // error counter
}

type DataStone struct {
	t uint32 // time
	v uint32 // version
	d []byte // data
	h []byte // hash(data)
}

func NewDrystone(curl *string, surl *string, urls *string) (stone *Drystone) {
	stone = &Drystone{
		CURL: *curl,
		SURL: *surl,
		WURLS:         make(map[string]*UrlStone),
		HeartBeatQuit: make(chan bool),
		data:          make(map[string]map[string]*DataStone),
		AddUrlsLimit:  3,
	}

	stone.addWURLs(urls)

	stone.buildName()

	go stone.run()

	return stone
}

func (stone *Drystone) addWURLs(urls *string) {
	stone.wuMux.Lock()
	defer stone.wuMux.Unlock()
	for _, u := range strings.Split(*urls, ",") {
		if _, ok := stone.WURLS[u]; ok {
			continue
		}
		stone.WURLS[u] = &UrlStone{ec: 0}
	}
}

func (stone *Drystone) delWURLs(urls *string) {
	stone.wuMux.Lock()
	defer stone.wuMux.Unlock()
	for _, u := range strings.Split(*urls, ",") {
		delete(stone.WURLS, u)
	}
}

func (stone *Drystone) getWURLs() string {
	stone.wuMux.Lock()
	defer stone.wuMux.Unlock()
	s := ""
	i := 0
	for u, _ := range stone.WURLS {
		if i == 0 {
			s = u
			i++
		} else {
			s = s + "," + u
		}
	}
	return s
}

func (stone *Drystone) buildName() string {
	cs := strings.Split(stone.CURL, ":")
	if len(cs) < 2 {
		panic(fmt.Sprintf("wrong client url %s", stone.CURL))
	}
	ss := strings.Split(stone.SURL, ":")
	if len(ss) < 2 {
		panic(fmt.Sprintf("wrong stone url %s", stone.SURL))
	}
	stone.nm = fmt.Sprintf("%s:[%s,%s]", cs[len(cs)-2], cs[len(cs)-1], ss[len(ss)-1])

	return stone.nm
}

func (stone *Drystone) run() {
	log.Printf("Drystone run [%s,%s]", stone.CURL, stone.SURL)

	go stone.serveClientHTTP()
	go stone.serveStoneHTTP()

	go stone.heartBeat()

}

// send other known stones heartbeat
func (stone *Drystone) updateWallUrls() {
}

func (stone *Drystone) heartBeat() {
	log.Printf("Drystone heartBeat [%s]", stone.nm)
	tick := time.Tick(StoneHeartBeatTimeInterval)
	var counter uint32 = 0
	for {
		select {
		case <-stone.HeartBeatQuit:
			log.Printf("stone.heartBeat exited by HeartBeatQuit %s %d", stone.nm, counter)
			return
		case <-tick:
			//log.Printf("stone.heartBeat tick %s %d", stone.nm, counter)
			counter++
			//stone.updateWallUrls()
		}
	}

}

//
// do global
//

type DataSlice []byte

const (
	doAdd = 1
	doGet = 2
	doDel = 3
)

func (stone *Drystone) doPrepareUrls(what int,urls *[]string){
	stone.wuMux.Lock()
	defer 	stone.wuMux.Unlock()
	if what == doAdd && len(stone.WURLS)>stone.AddUrlsLimit{
		nums:=make([]int,len(stone.WURLS))
		for i:=0;i<len(stone.WURLS);i++{
			nums[i]=i
		}
		nm:=make(map[int]struct{})
		for i:=0;i<stone.AddUrlsLimit;i++{
			ri:=rand.Intn(len(nums))
			nm[ri]=struct{}{}
			nums[ri]=nums[len(nums)-1]
			nums=nums[:len(nums)-1]
		}
		ix:=0
		for url, _ := range stone.WURLS  {
			if _,ok:=nm[ix];ok{
				*urls = append(*urls, url)
			}
			ix++
		}	

	} else {
		// del or get prepare all
		for url, _ := range stone.WURLS {
			*urls = append(*urls, url)
		}
	}
}


func (stone *Drystone) doGlobal(what int, g, k string, d []byte) (uint32, []byte) {

	var urls []string

	stone.doPrepareUrls(what,&urls)

	log.Printf("stone.doGlobal urls %v",urls)

//	stone.wuMux.Lock()
//	for url, _ := range stone.WURLS {
//		urls = append(urls, url)
//	}
//	stone.wuMux.Unlock()

	var c = len(urls)
	var wg sync.WaitGroup
	wg.Add(c)

	var od []DataSlice = make([]DataSlice, c)
	var ov []uint32 = make([]uint32, c)
	for i, u := range urls {
		go func(i int, u string) {
			switch what {
			case doAdd:
				ov[i], od[i] = stone.addStoneHttp(u, g, k, d)
			case doGet:
				ov[i], od[i] = stone.getStoneHttp(u, g, k)
			case doDel:
				ov[i], od[i] = stone.delStoneHttp(u, g, k)
			}
			wg.Done()
		}(i, u)
	}

	wg.Wait()

	return FindConsensus(ov, od)
}

//
// add data
//

func (stone *Drystone) addGlobal(g, k string, d []byte) (uint32, []byte) {
	return stone.doGlobal(doAdd, g, k, d)
}

func (stone *Drystone) addLocal(g, k string, d []byte) (uint32, []byte) {
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	var v uint32 = 1
	var ds *DataStone
	if _, ok = stone.data[g]; !ok {
		stone.data[g] = make(map[string]*DataStone)
	} else {
		if ds, ok = stone.data[g][k]; ok {
			v = ds.v + 1
		}
	}
	stone.data[g][k] = &DataStone{t: uint32(time.Now().Unix()), v: v, d: d, h: Hash(d)}
	if ds != nil {
		return ds.v, ds.d
	}
	return 0, nil
}

//
// get data
//

func (stone *Drystone) getGlobal(g, k string) (uint32, []byte) {
	return stone.doGlobal(doGet, g, k, nil)
}

func (stone *Drystone) getLocal(g, k string) (uint32, []byte) {
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	if _, ok = stone.data[g]; !ok {
		return 0, nil
	}
	var ds *DataStone
	if ds, ok = stone.data[g][k]; !ok {
		return 0, nil
	}
	return ds.v, ds.d
}

//
// del data
//

func (stone *Drystone) delGlobal(g, k string) (uint32, []byte) {
	return stone.doGlobal(doDel, g, k, nil)
}

func (stone *Drystone) delLocal(g, k string) (uint32, []byte) {
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	if _, ok = stone.data[g]; !ok {
		return 0, nil
	}
	var ds *DataStone
	if ds, ok = stone.data[g][k]; !ok {
		return 0, nil
	}
	delete(stone.data[g], k)
	return ds.v, ds.d
}
