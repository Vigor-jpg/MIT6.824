package mr

import (
	"fmt"
	"io/ioutil"
	"os"
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
	var res []KeyValue
	//fmt.Println(content)
	array := strings.Split(content, ",")
	for _, str := range array {
		//fmt.Println(str)
		kv := strings.Split(str, ":")
		res = append(res,KeyValue{kv[0],kv[1]})
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
