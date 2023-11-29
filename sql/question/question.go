package question

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

var (
	dbConnString   = "postgresql://postgres:1234@localhost:5432/stack-exchange?sslmode=disable"
	csvFile        = "./csv/Questions.csv"
	entityName     = `"questions"`
	dbMaxIdleConns = 4
	dbMaxConns     = 100
	totalWorker    = 100
	dataHeaders    = []string{
		`"id"`, `"owneruserid"`, `"creationdate"`, `"score"`, `"title"`, `"body"`,
	}
)

func openDBConnection() (*sql.DB, error) {
	log.Println("=> open db connection")

	db, err := sql.Open("postgres", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func openCSVFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("file %s tidak ditemukan", csvFile)
		}

		return nil, nil, err
	}

	reader := csv.NewReader(f)

	return reader, f, nil
}

func generateParams(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, fmt.Sprintf("$%d", i+1))
	}
	return s
}

func doTheJob(workerIndex int, counter int, db *sql.DB, values []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())
			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				entityName,
				strings.Join(dataHeaders, ","),
				strings.Join(generateParams(len(dataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, values...)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	if counter%100 == 0 {
		log.Printf("=> worker %d inserted %d data\n", workerIndex, counter)
	}
}

func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex < totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	isHeader := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if isHeader {
			isHeader = false
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {

			// Case specific: replace NA with NULL
			if each == "NA" {
				rowOrdered = append(rowOrdered, nil)
			} else {
				rowOrdered = append(rowOrdered, each)
			}

		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func Execute() {

	db, err := openDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCSVFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()
}
