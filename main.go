package main

import (
	"fmt"
	"sync"

	"github.com/faridtmammadov/bitcask/engine"
)

var wg sync.WaitGroup

func main() {
	diskStore, _ := engine.Open("testdb")
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			diskStore.Set("dostoyevski", "crime and punishment")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			value, err := diskStore.Get("dostoyevski")
			if err != nil {
				fmt.Println("Get error ", err)
				return
			}
			fmt.Println(value)
		}
	}()

	wg.Wait()

}
