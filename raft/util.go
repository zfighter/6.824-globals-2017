package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 2
const Truncate = true
const fileName = "debug.log"

var initialized = false

func Init() {
	if _, statErr := os.Stat(fileName); !os.IsNotExist(statErr) && Truncate {
		rmErr := os.Remove(fileName)
		if rmErr != nil {
			log.Printf("%s\n", rmErr.Error())
		}
	}
	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("%s\n", err.Error())
		os.Exit(-1)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if !initialized {
		Init()
		initialized = true
	}
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if !initialized {
		Init()
		initialized = true
	}
	if Debug > 1 {
		log.Printf(format, a...)
	}
	return
}
