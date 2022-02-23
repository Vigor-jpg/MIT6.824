package kvraft

import (
	"fmt"
	"io"
	"log"
	"os"
)

const Debug = false
const FileName = "../log/kvRaft.log"
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