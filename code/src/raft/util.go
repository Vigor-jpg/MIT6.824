package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
)

// Debugging
const Debug = false
const FileName = "../log/test.log"
func DPrintf(format string, a ...interface{}) (n int, err error) {
	msg := fmt.Sprintf(format,a...)
	if Debug {
		//log.Printf(format, a...)
		WriteLog(FileName,msg)
	}
	return
}
func DeleteLog()  {
	//file,err := os.Open(fileName)
	_ , err := os.Create(FileName)
	if err != nil{
		log.Fatal(err)
	}
}
func WriteLog(filePath string,str string) error{
	var (
		err error
		f   *os.File
	)
	f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	_, err = io.WriteString(f, "\n"+str)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	return err
}
func deepCopy(dst interface{},src interface{}) error{
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(src);err != nil{
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buff.Bytes())).Decode(dst)
}

func Min(a int,b int)  int{
	if a > b{
		return b
	}
	return a
}
func Max(a int, b int) int{
	if a > b{
		return a
    }
    return b
}
