package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func readFile(fileName string) string {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		fmt.Sprintf("Usage:can not open the file -----%s", fileName)
	}
	content, err := ioutil.ReadAll(file)
	return string(content)
}

func StringParse(content string) []KeyValue {
	maps := make(map[string]int)
	var res []KeyValue
	fmt.Println(content)
	array := strings.Split(content, ",")
	for _, str := range array {
		fmt.Println(str)
		kv := strings.Split(str, ":")
		num1, err2 := strconv.Atoi(kv[1])
		if err2 != nil {
			panic(err2)
		}
		num, ok := maps[kv[0]]
		if ok {
			maps[kv[0]] = num + num1
		} else {
			maps[kv[0]] = num1
		}
	}
	for k, v := range maps {
		num := strconv.Itoa(v)
		kv := KeyValue{
			Key:   k,
			Value: num,
		}
		res = append(res, kv)
	}
	return res
}
func writeFile(fileName string, content string) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		fmt.Sprintf("Usage:can not open the file -------%s", fileName)
	}
	file.WriteString(content)

}
