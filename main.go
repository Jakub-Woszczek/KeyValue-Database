package main

import (
	"bytes"
	"fmt"
)

func main() {
	key := []byte("myKey")
	value := []byte("myValue")

	buff := Encode(key,value)

	r := bytes.NewReader(buff)
	key,val,err := Decode(r)

	fmt.Println(key)
	fmt.Println(val)
	fmt.Println(err)
}
