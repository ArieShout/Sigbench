package util

import (
	"strconv"
)

func GetVMSSInstanceId(hostname string) (int64, error) {
    if len(hostname) <= 6 {
        return 0, nil
    }
	idPart := hostname[len(hostname) - 6:len(hostname)]
	return strconv.ParseInt(idPart, 36, 64)
}
