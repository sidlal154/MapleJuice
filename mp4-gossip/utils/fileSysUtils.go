package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func CheckDecimalValue(input string, value int) bool {
	targetValueStr := fmt.Sprintf("%02d#", value)
	return strings.Contains(input, targetValueStr)
}

func EmptyFromDirectory(dirPath string) {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return
	}
	for _, file := range files {
		path := dirPath + "/" + file.Name()

		if !file.IsDir() {
			err := os.Remove(path)
			if err != nil {
				return
			}
		}
	}
}
