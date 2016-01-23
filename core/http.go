package drystonedb

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	_"strings"
	_"sync"
	"time"
	"bytes"
)

const (
	StoneUpdateUrlTimeInterval = time.Second * 15
	StoneaddStoneTimeInterval = time.Second * 60 
)


// send hi to other stones
func (stone *Drystone) updateWallUrlHttp(u,me string){
	url:="http://"+u+"/update?s="+url.QueryEscape(me)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Printf("stone.addStoneHttp NewRequest url %s error %v", url,err)
		return
	}

	client := &http.Client{}
	_, err = client.Do(req)
	if err != nil {
		log.Printf("stone.addStoneHttp client.Do url %s error %v", url,err)
	}

}
/*
func (stone *Drystone) updateWallUrlHttp(u string){
	c := make(chan error, 1)

	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		log.Printf("stone.updateWallUrl http.NewRequest error %v, %v", err, req)
		return
	}

	tr := &http.Transport{}

	go func() {
		client := &http.Client{Transport: tr}
		resp, err := client.Do(req)

		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		c <- err
	}()

	select {
	case <-time.After(StoneUpdateUrlTimeInterval):
		tr.CancelRequest(req)
		log.Printf("stone.updateWallUrlHttp time after %d", (int)(StoneUpdateUrlTimeInterval/time.Second))
		<-c // Wait for goroutine to return.
		return
	case err := <-c:
		if err != nil {
			log.Printf("stone.updateWallUrlHttp case err := <-c error %v", err)
		}
		return
	}

}
*/

// curl -XPOST -v 'http://127.0.0.1:12379/data?g=boom&k=cambala' -d "cobra" 

func (stone *Drystone) addStoneHttp(u,g,k string, d []byte)([]byte){
 
	url:="http://"+u+"/stone?g="+url.QueryEscape(g)+"&k="+url.QueryEscape(k)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(d))
	if err != nil {
		log.Printf("stone.addStoneHttp NewRequest url %s error %v", url,err)
		return nil
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("stone.addStoneHttp client.Do url %s error %v", url,err)
		return nil
	}

	od, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.addStoneHttp ioutil.ReadAll url %s error %v", url,err)
		return nil
	}

	return od
}


// curl -XGET -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'

func (stone *Drystone) getStoneHttp(u,g,k string)([]byte){

	url:="http://"+u+"/stone?g="+url.QueryEscape(g)+"&k="+url.QueryEscape(k)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("stone.getStoneHttp NewRequest url %s error %v", url,err)
		return nil
	}

	//req.Header[k] = v

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("stone.getStoneHttp client.Do url %s error %v", url,err)
		return nil
	}

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.getStoneHttp ioutil.ReadAll url %s error %v", url,err)
		return nil
	}

	return d

}


func (stone *Drystone) delStoneHttp(u,g,k string)([]byte){

	url:="http://"+u+"/stone?g="+url.QueryEscape(g)+"&k="+url.QueryEscape(k)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("stone.delStoneHttp NewRequest url %s error %v", url,err)
		return nil
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("stone.delStoneHttp client.Do url %s error %v", url,err)
		return nil
	}

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.delStoneHttp ioutil.ReadAll url %s error %v", url,err)
		return nil
	}

	return d
}


// stone k/v interface
//
// curl -XPOST -v 'http://127.0.0.1:12379/data?g=boom&k=cambala' -d "cobra"
// curl -XGET -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'
// curl -XDELETE -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'
//

func getParamsFromRequest(r *http.Request) (g, k string) {
	g = r.URL.Query().Get("g")
	k = r.URL.Query().Get("k")
	return g, k
}

func (stone *Drystone) processPostRequest(w *http.ResponseWriter, r *http.Request, stOk bool) {
	g, k := getParamsFromRequest(r)
	log.Printf("stone post [%s] g=%s k=%s", stone.nm, g, k)
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

	if stOk {
		go stone.addGlobal(g, k, d)
	}

	(*w).WriteHeader(http.StatusOK)
	if od!=nil{
		(*w).Write(od)
	}
}

func (stone *Drystone) processGetRequest(w *http.ResponseWriter, r *http.Request, stOk bool){
	g, k := getParamsFromRequest(r)
	log.Printf("stone get [%s] g=%s k=%s", stone.nm, g, k)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	var od []byte

	if stOk {
		od=stone.getGlobal(g, k)
	} else {
		od=stone.getLocal(g, k)
	}

	(*w).WriteHeader(http.StatusOK)

	if od!=nil{
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
	} else {
		(*w).WriteHeader(http.StatusNotFound)
	}
}

func (stone *Drystone) processDeleteRequest(w *http.ResponseWriter, r *http.Request, stOk bool) {
	g, k := getParamsFromRequest(r)
	log.Printf("stone delete [%s] g=%s k=%s", stone.nm, g, k)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	var od []byte

	if stOk {
		od=stone.delGlobal(g, k)
	} else{
		od=stone.delLocal(g, k)
	}

	if od!=nil{
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
	} else {
		(*w).WriteHeader(http.StatusNotFound)
	}
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
			c.stone.processPostRequest(&w, r, false)
			return
		}
	}

	if r.Method == "GET" {
		if r.URL.Path == "/data" {
			c.stone.processGetRequest(&w, r, false)
			return
		}

	}

	if r.Method == "DELETE" {
		if r.URL.Path == "/data" {
			c.stone.processDeleteRequest(&w, r, false)
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
			e.stone.processPostRequest(&w, r, true)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/stone" {
			e.stone.processGetRequest(&w, r, true)
			return
		}

	}

	if r.Method == "DELETE" {
		if r.URL.Path == "/stone" {
			e.stone.processDeleteRequest(&w, r, true)
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
