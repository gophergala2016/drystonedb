package drystonedb

import (
	"fmt"
)

//
// consensus, probably semicorrect %)
//

func FindConsensus(ov []uint32, od []DataSlice) (uint32, []byte) {
	if len(od) != len(ov) || len(od) == 0 || len(ov) == 0 {
		panic(fmt.Sprintf("FindConsensus error papams: %v,%v", ov, od))
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
	// 2nd
	m := make(map[int]int)
	for i, v := range ov {
		if v == vmax {
			m[i]++
		}
	}
	// 3d
	imax = 0
	var vimax = 0
	for k, v := range m {
		if v > vimax {
			vimax = v
			imax = k
		}
	}
	return ov[imax], od[imax]
}
