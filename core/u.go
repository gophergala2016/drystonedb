package drystonedb

import (
	_ "bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

func ReadJSON(r *http.Request, v interface{}) ([]byte, error) {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return content, err
	}
	if err := json.Unmarshal(content, v); err != nil {
		return content, err
	}
	return content, nil
}

func NewUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func compSlices(a, b []byte) bool {
	if len(a) == len(b) {
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}

func Hash(data []byte) []byte {
	hasher := sha256.New()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func Hash_2x(b []byte) []byte {
	hasher := sha256.New()
	hasher.Write(b)
	sum := hasher.Sum(nil)
	hasher.Reset()
	hasher.Write(sum)
	return hasher.Sum(nil)
}
