package drystonedb

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/url"
	"strings"
	"sync"
	"time"
)

const (
	StoneHeartBeatTimeInterval = time.Second * 5
)

type Drystone struct {
	CURL  string // client url
	SURL  string // internal url
	URLS  []string // urls list
	nm    string // name
	HeartBeatQuit chan bool
	data  map[string]map[string]*DataStone // data storage
	daMux sync.Mutex // one mux for all, not perfect but...
}

type DataStone struct {
	t uint32 // time
	v uint32 // version
	d []byte // data
}

func NewDrystone(curl *string, surl *string, urls *string) (stone *Drystone) {
	stone = &Drystone{
		CURL: *curl,
		SURL: *surl,
		URLS: strings.Split(*urls,","),
		HeartBeatQuit: make(chan bool),
		data: make(map[string]map[string]*DataStone),
	}

	stone.name()

	go stone.run()

	return stone
}

func (stone *Drystone) name() string {
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

}

func (stone *Drystone) heartBeat() {
	log.Printf("Drystone heartBeat [%s]", stone.nm)
	tick := time.Tick(StoneHeartBeatTimeInterval)
	var counter uint32 = 0
	for {
		select {
		case <-stone.HeartBeatQuit:
			log.Printf("stone.heartBeat exited by HeartBeatQuit %s %d",stone.nm, counter)
			return
		case <-tick:
			log.Printf("stone.heartBeat tick %s %d", stone.nm, counter)
			counter++
		}
	}

}


func (stone *Drystone) addLocal(g, k string, d []byte) []byte{
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	var gd map[string]*DataStone 
	var od []byte
	var v uint32
	if gd, ok = stone.data[g]; !ok {
		gd = make(map[string]*DataStone)
		stone.data[g]=gd
	} else {
		v=gd[k].v+1
		od=gd[k].d
	}
	gd[k]=&DataStone{t:uint32(time.Now().Unix()),v:v,d:d}
	return od
}

func (stone *Drystone) getLocal(g, k string) []byte {
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	var gd map[string]*DataStone 
	var od []byte
	if gd, ok = stone.data[g]; !ok {
		return nil
	}
	if _, ok = gd[k]; !ok {
		return nil
	}
	od=gd[k].d
	return od
}

func (stone *Drystone) delLocal(g, k string) []byte{
	stone.daMux.Lock()
	defer stone.daMux.Unlock()
	var ok bool
	var gd map[string]*DataStone 
	var od []byte
	if gd, ok = stone.data[g]; !ok {
		return nil
	}
	if _, ok = gd[k]; !ok {
		return nil
	}
	od=gd[k].d
	delete(gd,k)
	return od
}

// stone k/v interface
//
// curl -v -XPOST -d '123456789abcdef!' 'http://localhost:8080/data?g=test&k=key1
// curl -v -XGET 'http://localhost:8080/data?g=test&k=key1'
// curl -v -XDELETE 'http://localhost:8080/data/mydata?g=test&k=key1'
//

func getParamsFromRequest(r *http.Request) (g, k string) {
	g = r.URL.Query().Get("g")
	k = r.URL.Query().Get("k")
	return g, k
}

func (stone *Drystone) processClientPostRequest(w *http.ResponseWriter, r *http.Request) {
	g, k := getParamsFromRequest(r)
	log.Printf("Clientstone post [%s] g=%s k=%s", stone.nm, g, k)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}
	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(*w, fmt.Sprintf("bad reguest, error %v", err), http.StatusBadRequest)
		return
	}

	od:=stone.addLocal(g, k, d)

	(*w).WriteHeader(http.StatusOK)
	if od!=nil{
		(*w).Write(od)
	}
}

func (stone *Drystone) processClientGetRequest(w *http.ResponseWriter, r *http.Request) {
	g, k := getParamsFromRequest(r)
	log.Printf("Clientstone get [%s] g=%s k=%s", stone.nm, g, k)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	od:=stone.getLocal(g, k)


	(*w).WriteHeader(http.StatusOK)

	if od!=nil{
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
	} else {
		(*w).WriteHeader(http.StatusNotFound)
	}
}

func (stone *Drystone) processClientDeleteRequest(w *http.ResponseWriter, r *http.Request) {
	g, k := getParamsFromRequest(r)
	log.Printf("Clientstone delete [%s] g=%s k=%s", stone.nm, g, k)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}


	od:=stone.delLocal(g, k)
	

	if od!=nil{
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
	} else {
		(*w).WriteHeader(http.StatusNotFound)
	}
}

func (stone *Drystone) processStonePostRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processStonePostRequest %s", stone.nm)
}

func (stone *Drystone) processStoneGetRequest(w *http.ResponseWriter, r *http.Request) {
	log.Printf("Drystone processStoneGetRequest %s", stone.nm)
}

// boom
// serve client nodes http requests
// http2 is cool, but later

type ClientHttp struct {
	stone *Drystone
}

func (c *ClientHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("ClientHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	if r.Method == "POST" {
		if r.URL.Path == "/data" {
			c.stone.processClientPostRequest(&w, r)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/data" {
			c.stone.processClientGetRequest(&w, r)
			return
		}

	}

	if r.Method == "DELETE" {
		if r.URL.Path == "/data" {
			c.stone.processClientDeleteRequest(&w, r)
			return
		}

	}

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (stone *Drystone) serveClientHTTP() {
	log.Printf("stone.serveClientHTTP %s", stone.CURL)
	err := http.ListenAndServe(stone.CURL, &ClientHttp{stone: stone})
	if err != nil {
		log.Fatal("stone.serveClientHTTP error", err)
	}
}

// boom
// serve stone nodes http requests
// http2 is cool, but later

type DrystoneHttp struct {
	stone *Drystone
}

func (e *DrystoneHttp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("ClientHttp.ServeHTTP: %s %s %s", r.RemoteAddr, r.Method, r.URL)

	if r.Method == "POST" {
		if r.URL.Path == "/stone" {
			e.stone.processStonePostRequest(&w, r)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/stone" {
			e.stone.processStoneGetRequest(&w, r)
			return
		}

	}

	http.Error(w, "bad reguest", http.StatusBadRequest)
}

func (stone *Drystone) serveStoneHTTP() {
	log.Printf("stone.serveStoneHTTP %s", stone.SURL)
	err := http.ListenAndServe(stone.SURL, &DrystoneHttp{stone: stone})
	if err != nil {
		log.Fatal("stone.serveStoneHTTP error", err)
	}
}
