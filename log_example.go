//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/milossdjuric/logstream/internal/storage"
)

func main() {
	l, err := storage.NewLog("./logs", 64)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	off1, _ := l.Append([]byte("hello"))
	off2, _ := l.Append([]byte("world"))

	r1, _ := l.Read(off1)
	r2, _ := l.Read(off2)

	fmt.Println(off1, string(r1))
	fmt.Println(off2, string(r2))
}
