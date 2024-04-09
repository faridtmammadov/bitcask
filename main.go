package main

import (
	"fmt"

	"github.com/faridtmammadov/bitcask/engine"
)

func main() {
	diskStore, _ := engine.NewDiskStore("testdb")

	diskStore.Set("dostoyevski", "crime and punishment")

	value, err := diskStore.Get("dostoyevski")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(value)
}
