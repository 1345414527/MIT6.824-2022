package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

var file *os.File

func init() {
	f, err := os.Create("./tmp/log-" + strconv.Itoa(int(time.Now().Unix())) + ".txt")
	if err != nil {
		DPrintf("log create file fail!")
		fmt.Println("log create file fail!")
	}
	file = f
}

//debug下打印日志
func DPrintf(format string, value ...interface{}) {
	now := time.Now()
	info := fmt.Sprintf("%v-%v-%v %v:%v:%v:  ", now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second()) + fmt.Sprintf(format+"\n", value...)

	if Debug {
		log.Printf(info)
	} else {
		file.WriteString(info)
	}
}
