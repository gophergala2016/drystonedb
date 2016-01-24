package drystonedb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	_ "strings"
	_ "sync"
	"time"
)

const (
	DrystoneVersionHeader      = "X-Drystonedb-Version"
	StoneUpdateUrlTimeInterval = time.Second * 15
	StoneaddStoneTimeInterval  = time.Second * 60
)

/*
// send hi to other stones
func (stone *Drystone) updateWallUrlHttp(u, me string) {
	url := "http://" + u + "/update?s=" + url.QueryEscape(me)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Printf("stone.updateWallUrlHttp NewRequest url %s error %v", url, err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("stone.updateWallUrlHttp client.Do url %s error %v", url, err)
		return
	}
	resp.Body.Close()

}
*/
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

func getDrystoneVersionHeader(resp *http.Response) uint32 {

	var ok bool
	var s []string
	var v uint32
	if s, ok = resp.Header[DrystoneVersionHeader]; ok {
		if len(s) > 0 {
			u64, err := strconv.ParseUint(s[0], 10, 32)
			if err != nil {
				log.Printf("resp.Header ParseUint error %v", err)
				return 0
			}
			v = uint32(u64)
		}
	}
	return v
}

// curl -XPOST -v 'http://127.0.0.1:12379/data?g=boom&k=cambala' -d "cobra"

func (stone *Drystone) addStoneHttp(u, g, k string, d []byte) (uint32, []byte) {

	url := "http://" + u + "/stone?g=" + url.QueryEscape(g) + "&k=" + url.QueryEscape(k)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(d))
	if err != nil {
		log.Printf("stone.addStoneHttp NewRequest url %s error %v", url, err)
		return 0, nil
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	if err != nil {
		log.Printf("stone.addStoneHttp client.Do url %s error %v", url, err)
		return 0, nil
	}

	if resp.StatusCode != http.StatusOK {
		//		log.Printf("stone.addStoneHttp client.Do url %s resp.StatusCode %d", url, resp.StatusCode)
		return 0, nil
	}

	v := getDrystoneVersionHeader(resp)

	od, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.addStoneHttp ioutil.ReadAll url %s error %v", url, err)
		return 0, nil
	}

	return v, od
}

// curl -XGET -v 'http://127.0.0.1:12379/data?g=boom&k=cambala'

func (stone *Drystone) getStoneHttp(u, g, k string) (uint32, []byte) {

	url := "http://" + u + "/stone?g=" + url.QueryEscape(g) + "&k=" + url.QueryEscape(k)
	resp, err := http.Get(url)
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	if err != nil {
		log.Printf("stone.getStoneHttp NewRequest url %s error %v", url, err)
		return 0, nil
	}

	if resp.StatusCode != http.StatusOK {
		//log.Printf("stone.getStoneHttp url %s resp.StatusCode %d", url, resp.StatusCode)
		return 0, nil
	}

	v := getDrystoneVersionHeader(resp)

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.getStoneHttp ioutil.ReadAll url %s error %v", url, err)
		return 0, nil
	}

	return v, d

}

func (stone *Drystone) delStoneHttp(u, g, k string) (uint32, []byte) {

	url := "http://" + u + "/stone?g=" + url.QueryEscape(g) + "&k=" + url.QueryEscape(k)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("stone.delStoneHttp NewRequest url %s error %v", url, err)
		return 0, nil
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		log.Printf("stone.delStoneHttp client.Do url %s error %v", url, err)
		return 0, nil
	}

	if resp.StatusCode != http.StatusOK {
		return 0, nil
	}

	v := getDrystoneVersionHeader(resp)

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("stone.delStoneHttp ioutil.ReadAll url %s error %v", url, err)
		return 0, nil
	}

	return v, d
}

// stone url interface
//
// curl -XPOST -v 'http://127.0.0.1:12379/urls' -d "127.0.0.1:8009,127.0.0.1:8010"
// curl -XGET -v 'http://127.0.0.1:12379/urls'
// curl -XDELETE -v 'http://127.0.0.1:12379/urls' -d "127.0.0.1:8009,127.0.0.1:8010"
//

func (stone *Drystone) processUrlPostRequest(w *http.ResponseWriter, r *http.Request) {
	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(*w, fmt.Sprintf("bad reguest, error %v", err), http.StatusBadRequest)
		return
	}
	urls := string(d)
	stone.addWURLs(&urls)
	(*w).WriteHeader(http.StatusOK)
}

func (stone *Drystone) processUrlGetRequest(w *http.ResponseWriter, r *http.Request) {
	(*w).WriteHeader(http.StatusOK)
	(*w).Write([]byte(stone.getWURLs()))
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
	log.Printf("stone post [%s] g=%s k=%s st=%t", stone.nm, g, k, stOk)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	d, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(*w, fmt.Sprintf("bad reguest, error %v", err), http.StatusBadRequest)
		return
	}

	//ov,od:=stone.addLocal(g, k, d)
	var ov uint32 // old version
	var od []byte // old data

	if stOk {
		ov, od = stone.addLocal(g, k, d)
	} else {
		ov, od = stone.addGlobal(g, k, d)
	}

	if od != nil {
		(*w).Header().Set(DrystoneVersionHeader, fmt.Sprintf("%d", ov))
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
		return
	}

	(*w).WriteHeader(http.StatusOK)
}

func (stone *Drystone) processGetRequest(w *http.ResponseWriter, r *http.Request, stOk bool) {
	g, k := getParamsFromRequest(r)
	log.Printf("stone get [%s] g=%s k=%s st=%t", stone.nm, g, k, stOk)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	var ov uint32 // old version
	var od []byte // old data

	if stOk {
		ov, od = stone.getLocal(g, k)
	} else {
		ov, od = stone.getGlobal(g, k)
	}

	//(*w).WriteHeader(http.StatusOK)

	if od != nil {
		//log.Printf("stone.processGetRequest ov=%d od=%v",ov,od)
		(*w).Header().Set(DrystoneVersionHeader, fmt.Sprintf("%d", ov))
		(*w).WriteHeader(http.StatusOK)
		(*w).Write(od)
	} else {
		(*w).WriteHeader(http.StatusNotFound)
	}
}

func (stone *Drystone) processDeleteRequest(w *http.ResponseWriter, r *http.Request, stOk bool) {
	g, k := getParamsFromRequest(r)
	log.Printf("stone delete [%s] g=%s k=%s st=%t", stone.nm, g, k, stOk)
	if g == "" || k == "" {
		http.Error(*w, "bad reguest", http.StatusBadRequest)
		return
	}

	var ov uint32 // old version
	var od []byte // old data

	if stOk {
		ov, od = stone.delLocal(g, k)
	} else {
		ov, od = stone.delGlobal(g, k)
	}

	if od != nil {
		(*w).Header().Set(DrystoneVersionHeader, fmt.Sprintf("%d", ov))
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

		if r.URL.Path == "/urls" {
			e.stone.processUrlPostRequest(&w, r)
			return
		}

	}

	if r.Method == "GET" {
		if r.URL.Path == "/stone" {
			e.stone.processGetRequest(&w, r, true)
			return
		}
		if r.URL.Path == "/urls" {
			e.stone.processUrlGetRequest(&w, r)
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
