package main

import (
	"fmt"

	"github.com/faridtmammadov/bitcask/engine"
)

func main() {
	diskStore, _ := engine.Open("testdb")

	v, err := diskStore.Get("dostoyevski")
	if err != nil {
		fmt.Println("Get error ", err)
		return
	}

	fmt.Println(v)
}
