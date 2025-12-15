package protocol

import (
	"fmt"
	"testing"
)

func TestPrint(t *testing.T) {
	fmt.Println(NewProduceMessage("topic-1", []byte("hello")))
	fmt.Println(NewConsumeMessage("topic-1", 5))
	fmt.Println(NewMessage(TypeData, []byte("data data data")))
}
