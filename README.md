# Go Downloader

CLI-загрузчик файлов на Go с поддержкой параллельной загрузки, докачки после прерывания и graceful shutdown.

## Возможности

* Параллельная загрузка чанков через worker pool
* Разбиение файла на чанки по 10 MB
* Докачка незавершённой загрузки
* Сохранение состояния загрузки в `.progress`
* HTTP Range Requests
* Повторные попытки загрузки при ошибках
* До 8 одновременно загружаемых чанков
* Progress bar с отображением процента и объёма данных
* Graceful shutdown при `Ctrl+C`
* Одновременная загрузка нескольких файлов

## Требования

* Go 1.21+
* Сервер должен поддерживать HTTP Range Requests для докачки файлов

## Использование

```bash
go run . <директория> <url1> [url2...]
```

Например:

```bash
go run . ./downloads https://example.com/file.zip
```

Несколько файлов:

```bash
go run . ./downloads \
    https://example.com/file1.zip \
    https://example.com/file2.zip
```

## Сборка

Собрать исполняемый файл:

```bash
go build -o downloader .
```

Запустить:

```bash
./downloader ./downloads https://example.com/file.zip
```

Для Windows:

```bash
go build -o downloader.exe .
```

```powershell
.\downloader.exe .\downloads https://example.com/file.zip
```

## Как работает загрузка

Файл разбивается на чанки фиксированного размера — 10 MB.

Каждый chunk загружается отдельным HTTP-запросом с использованием `Range`:

```text
Range: bytes=0-10485759
```

Для выполнения загрузок используется worker pool:

```text
                    Downloader
                        │
                        ▼
                      jobs
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       Worker 1      Worker 2      Worker 3 ... Worker 8
          │             │             │
          ▼             ▼             ▼
       Chunk 1        Chunk 2        Chunk 3
          │             │             │
          └─────────────┼─────────────┘
                        ▼
                    output file
```

После успешной загрузки каждого чанка его состояние сохраняется в `.progress`.

Если программа была прервана, при следующем запуске уже загруженные чанки пропускаются.

## Graceful Shutdown

При нажатии `Ctrl+C` приложение:

1. Получает `os.Interrupt`
2. Отменяет общий `context`
3. Перестаёт выдавать новые задачи worker'ам
4. Позволяет уже выполняющимся загрузкам завершиться
5. Сохраняет актуальное состояние
6. Корректно завершает работу

Таким образом, незавершённую загрузку можно продолжить при следующем запуске.

## Retry

Если загрузка чанка завершилась ошибкой, приложение повторяет запрос до 3 раз.

Между попытками используется задержка в 2 секунды.

## Структура проекта

```text
.
├── main.go
├── go.mod
├── go.sum
├── README.md
└── .gitignore
```

## Технологии

* Go
* `net/http`
* HTTP Range Requests
* Goroutines
* Channels
* Worker Pool
* `sync.WaitGroup`
* `sync.Mutex`
* `context`
* `os/signal`
* JSON
* `mpb/v8`

## Лицензия

Проект создан в учебных целях.
