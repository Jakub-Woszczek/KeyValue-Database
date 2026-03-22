package main

import (
	"fmt"
	"github.com/Jakub-Woszczek/kvdb/serializer"
)

func main() {
	key := []byte("myKey")
	value := []byte("myValue")

	buff := serializer.Encode(key,value)

	key,val,err := serializer.Decode(buff)

	fmt.Println(key)
	fmt.Println(val)
	fmt.Println(err)
}
