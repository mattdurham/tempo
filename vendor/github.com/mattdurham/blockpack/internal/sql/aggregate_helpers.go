package sql

import "strconv"

func percentileFromFuncName(funcName string) (float64, bool) {
	if len(funcName) < 2 || funcName[0] != 'P' {
		return 0, false
	}

	percent, err := strconv.Atoi(funcName[1:])
	if err != nil || percent < 0 || percent > 100 {
		return 0, false
	}

	return float64(percent) / 100.0, true
}
