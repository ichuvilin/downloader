package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]
	fmt.Printf("Директория для скачивания: %s\n", savePath)
	fmt.Println("URL для скачивания")
	for _, u := range urls {
		fmt.Printf("\t- %s\n", u)
	}
}
