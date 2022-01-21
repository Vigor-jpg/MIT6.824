package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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
