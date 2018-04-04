package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 1
const Truncate = true
const fileName = "debug.log"

func Init() {
	if _, statErr := os.Stat(fileName); !os.IsNotExist(statErr) && Truncate {
		rmErr := os.Remove(fileName)
		if rmErr != nil {
			log.Printf("%s\n", rmErr.Error())
		}
	}
	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	logger := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fileName := "debug.log"
		if err != nil {
			log.Printf("%s\n", err.Error())
			os.Exit(-1)
		}
		logger.Printf(format, a...)
	}
	return
}
