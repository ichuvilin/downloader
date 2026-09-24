package main

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const chunkSize = 10 * 1024 * 1024

func downloadFile(url, savePath string) error {
	err := os.MkdirAll(savePath, 0755)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Head(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	contentLength := resp.Header.Get("Content-Length")
	size, _ := strconv.ParseInt(contentLength, 10, 64)

	// Поддержка докачки
	acceptRanges := resp.Header.Get("Accept-Ranges")
	supportsResume := acceptRanges == "bytes"

	fmt.Printf("Размер:  %d\n", size)
	fmt.Printf("Докачка: %t\n", supportsResume)

	resp, err = http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("серер вернул %d", resp.StatusCode)
	}

	totalChunks := (size + chunkSize - 1) / chunkSize
	fileName := filepath.Join(savePath, path.Base(url))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	err = file.Truncate(size)
	if err != nil {
		return err
	}

	for i := int64(0); i < totalChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize - 1

		if end >= size {
			end = size - 1
		}

		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusPartialContent {
			return fmt.Errorf("сервер вернул: %d", resp.StatusCode)
		}
		defer resp.Body.Close()
	}

	//_, err = io.Copy(file, resp.Body)
	//if err != nil {
	//	return err
	//}
	fmt.Printf("Файл сохранён: %s\n", fileName)
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	var wg sync.WaitGroup

	fmt.Printf("Директория для скачивания: %s\n", savePath)
	for _, u := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			if err := downloadFile(u, savePath); err != nil {
				fmt.Println(fmt.Errorf("error during download %s, %w", u, err))
			}
		}(u)
	}
	wg.Wait()
}
