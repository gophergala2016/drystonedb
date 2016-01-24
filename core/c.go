package drystonedb

import (
	"fmt"
	_"log"
)

//
// consensus, probably semicorrect %)
//

func FindConsensus(ov []uint32, od []DataSlice) (uint32, []byte) {
	//log.Printf("FindConsensus %v,%v", ov, od)
	if len(od) != len(ov) || len(od) == 0 || len(ov) == 0 {
		panic(fmt.Sprintf("FindConsensus error papams: %d,%v", ov, od))
	}
	var imax = 0
	var vmax uint32 = 0
	// 1st
	for i, v := range ov {
		if v > vmax {
			vmax = v
			imax = i
		}
	}

	if vmax == 0{
		return 0,nil
	}
	//log.Printf("FindConsensus vmax=%d,imax=%d", vmax, imax)

	// 2nd
	m := make(map[int]int)
	for i, v := range ov {
		if v == vmax {
			m[i]++
		}
	}
	//log.Printf("FindConsensus len(m)=%d", len(m))
	// 3d
	imax = 0
	var vimax = 0
	for k, v := range m {
		if v > vimax {
			vimax = v
			imax = k
		}
	}
	//log.Printf("FindConsensus imax=%d, ov=%d,od=%s", imax,ov[imax], string(od[imax]))
	return ov[imax], od[imax]
}
