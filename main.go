package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
)

func downloadFile(url, savePath string) error {
	err := os.MkdirAll(savePath, 0644)
	if err != nil {
		return err
	}
	fmt.Printf("Скачивание: %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("серер вернул %d", resp.StatusCode)
	}

	fileName := filepath.Join(savePath, path.Base(url))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("Файл сохранён: %s\n", file)
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]
	fmt.Printf("Директория для скачивания: %s\n", savePath)
	for _, u := range urls {
		if err := downloadFile(u, savePath); err != nil {
			fmt.Errorf("error during download %s, %w", u, err)
		}
	}
}
