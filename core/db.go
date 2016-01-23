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
	CURL  string // client url
	SURL  string // internal url
	WURLS  []string // wall urls list
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
		WURLS: strings.Split(*urls,","),
		HeartBeatQuit: make(chan bool),
		data: make(map[string]map[string]*DataStone),
	}

	stone.buildName()

	go stone.run()

	return stone
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
func (stone *Drystone) updateWallUrls(){
	for _,u:=range stone.WURLS {
		stone.updateWallUrlHttp(u, stone.SURL)
	}
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
			stone.updateWallUrls()
		}
	}

}


func (stone *Drystone) addGlobal(g, k string, d []byte) (od []byte){
	for _,u:=range stone.WURLS {
		od=stone.addStoneHttp(u,g,k,d)
	}
	return od
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

func (stone *Drystone) getGlobal(g, k string) (d []byte) {
	for _,u:=range stone.WURLS {
		d=stone.getStoneHttp(u,g,k)
	}
	return d
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

func (stone *Drystone) delGlobal(g, k string) (d []byte) {
	for _,u:=range stone.WURLS {
		d=stone.delStoneHttp(u,g,k)
	}
	return d
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

