package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	chunkSize = 10 * 1024 * 1024

	maxRetries = 3
	retryDelay = 2 * time.Second

	maxConcurrentChunks = 8
)

type DownloadState struct {
	URL              string `json:"url"`
	TotalSize        int64  `json:"total_size"`
	ChunkSize        int    `json:"chunk_size"`
	TotalChunks      int    `json:"total_chunks"`
	DownloadedChunks []bool `json:"downloaded_chunks"`
}

func downloadFile(url, savePath string) error {
	err := os.MkdirAll(savePath, 0755)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}

	fileName := filepath.Join(savePath, path.Base(url))

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
	totalChunks := (size + chunkSize - 1) / chunkSize

	var state *DownloadState

	if _, err = os.Stat(fmt.Sprintf("%s.progress", fileName)); err == nil {
		data, _ := os.ReadFile(fmt.Sprintf("%s.progress", fileName))
		err := json.Unmarshal(data, &state)
		if err != nil {
			return err
		}
	} else if os.IsNotExist(err) {
		state = &DownloadState{
			URL:              url,
			TotalSize:        size,
			ChunkSize:        chunkSize,
			TotalChunks:      int(totalChunks),
			DownloadedChunks: make([]bool, int(totalChunks)),
		}
	} else {
		return err
	}

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

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	err = file.Truncate(size)
	if err != nil {
		return err
	}

	if err = SaveState(fileName, state); err != nil {
		return err
	}

	jobs := make(chan int64)
	var wg sync.WaitGroup

	var fileMu sync.Mutex
	var stateMu sync.Mutex

	for w := 0; w < maxConcurrentChunks; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for chunkID := range jobs {
				start := chunkID * chunkID
				end := start + chunkSize + 1
				if end >= size {
					end = size - 1
				}

				for attempt := 0; attempt < maxRetries; attempt++ {
					err := downloadChunk(url, start, end, client, file, &fileMu)

					if err == nil {
						stateMu.Lock()

						state.DownloadedChunks[chunkID] = true

						err = SaveState(fileName, state)

						stateMu.Unlock()

						if err != nil {
							fmt.Printf(
								"Ошибка сохранения состояния чанка %d: %v\n",
								chunkID+1,
								err,
							)
						}

						break
					}

					if attempt < maxRetries-1 {
						fmt.Printf(
							"Ошибка чанка %d, повтор через %v...\n",
							chunkID+1,
							retryDelay,
						)

						time.Sleep(retryDelay)
					} else {
						fmt.Printf(
							"Чанк %d не удалось загрузить: %v\n",
							chunkID+1,
							err,
						)
					}
				}
			}
		}()
	}

	for i := int64(0); i < totalChunks; i++ {
		stateMu.Lock()
		downloaded := state.DownloadedChunks[i]
		stateMu.Unlock()
		if downloaded {
			fmt.Printf(
				"Чанк %d уже загружен, пропускаем\n",
				i+1,
			)
			continue
		}
		jobs <- i
	}

	close(jobs)

	wg.Wait()

	fmt.Printf("Файл сохранён: %s\n", fileName)
	return nil
}

func downloadChunk(url string, start int64, end int64, client *http.Client, file *os.File, fileMu *sync.Mutex) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set(
		"Range",
		fmt.Sprintf("bytes=%d-%d", start, end),
	)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf(
			"сервер вернул: %d",
			resp.StatusCode,
		)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fileMu.Lock()
	defer fileMu.Unlock()

	_, err = file.WriteAt(data, start)
	if err != nil {
		return err
	}

	return nil
}

func SaveState(filename string, state *DownloadState) error {
	data, err := json.MarshalIndent(state, "", " ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(fmt.Sprintf("%s.progress", filename), data, 0644); err != nil {
		return err
	}

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
