package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
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

func downloadFile(ctx context.Context, url, savePath string, p *mpb.Progress) error {
	if err := os.MkdirAll(savePath, 0755); err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	fileName := filepath.Join(savePath, path.Base(url))

	resp, err := client.Head(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	contentLength := resp.Header.Get("Content-Length")

	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return err
	}

	acceptRanges := resp.Header.Get("Accept-Ranges")
	supportsResume := acceptRanges == "bytes"

	totalChunks := (size + chunkSize - 1) / chunkSize

	var state *DownloadState

	progressFile := fmt.Sprintf("%s.progress", fileName)

	if _, err = os.Stat(progressFile); err == nil {
		data, err := os.ReadFile(progressFile)
		if err != nil {
			return err
		}

		state = &DownloadState{}

		if err := json.Unmarshal(data, state); err != nil {
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

	// Считаем уже загруженные байты.
	var downloadedBytes int64

	for i, downloaded := range state.DownloadedChunks {
		if !downloaded {
			continue
		}

		start := int64(i) * int64(state.ChunkSize)
		end := start + int64(state.ChunkSize)

		if end > state.TotalSize {
			end = state.TotalSize
		}

		downloadedBytes += end - start
	}

	// Создаём progress bar.
	bar := p.AddBar(
		size,
		mpb.PrependDecorators(
			decor.Name(path.Base(fileName)),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.CountersKibiByte("% .2f / % .2f"),
		),
	)

	// Восстанавливаем прогресс.
	if downloadedBytes > 0 {
		bar.SetCurrent(downloadedBytes)
	}

	fmt.Printf("Размер: %d\n", size)
	fmt.Printf("Докачка: %t\n", supportsResume)

	file, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(size); err != nil {
		return err
	}

	if err := SaveState(fileName, state); err != nil {
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

			for {
				select {
				case <-ctx.Done():
					return
				case chunkID, ok := <-jobs:
					if !ok {
						return
					}

					start := chunkID * chunkSize
					end := start + chunkSize - 1

					if end >= size {
						end = size - 1
					}

					for attempt := 0; attempt < maxRetries; attempt++ {
						err := downloadChunk(
							url,
							start,
							end,
							client,
							file,
							&fileMu,
						)

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

							// Обновляем progress bar.
							bar.IncrBy(int(end - start + 1))

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
			}
		}()
	}

sendJobs:
	for i := int64(0); i < totalChunks; i++ {
		stateMu.Lock()
		downloaded := state.DownloadedChunks[i]
		stateMu.Unlock()

		if downloaded {
			continue
		}

		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- i:
		}

	}

	close(jobs)

	wg.Wait()

	stateMu.Lock()
	err = SaveState(fileName, state)
	stateMu.Unlock()

	if err != nil {
		return err
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt)
	defer signal.Stop(sigChan)

	go func() {
		<-sigChan

		fmt.Println("\nПолучен сигнал прерывания, завершаем...")
		cancel()
	}()

	savePath := os.Args[1]
	urls := os.Args[2:]

	p := mpb.New()

	var wg sync.WaitGroup

	fmt.Printf("Директория для скачивания: %s\n", savePath)
	for _, u := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			if err := downloadFile(ctx, u, savePath, p); err != nil {
				fmt.Println(fmt.Errorf("error during download %s, %w", u, err))
			}
		}(u)
	}
	wg.Wait()
	p.Wait()
}
