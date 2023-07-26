package raft

import (
	"log"
)

// Debugging
const Debug = 0
const Debug2 = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 > 0 {
		log.Printf(format, a...)
	}
	return
}
