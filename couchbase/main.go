package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

var (
	dbConnString = "couchbase://localhost"
	username     = "Administrator"
	password     = "password"
	jsonFile     = "./json/data.json"
	totalWorker  = 100
)

type Question struct {
	ID          uint   `json:"id"`
	RawQuestion string `json:"question"`
}

func openDBConnection() (*gocb.Cluster, error) {
	log.Println("=> open db connection")

	// Connect to Couchbase Server
	cluster, err := gocb.Connect(dbConnString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})

	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func openJSONFile() (*json.Decoder, *os.File, error) {
	log.Println("=> open json file")

	f, err := os.Open(jsonFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("file %s tidak ditemukan", jsonFile)
		}

		return nil, nil, err
	}

	decoder := json.NewDecoder(f)

	return decoder, f, nil
}

func readJSONFilePerObjectThenSendToWorker(jsonDecoder *json.Decoder, jobs chan<- Question, wg *sync.WaitGroup) {
	// Read opening delimiter. `[` or `{`
	if _, err := jsonDecoder.Token(); err != nil {
		log.Fatalf("%v", err)
	}

	// Read file content as long as there is something.
	for jsonDecoder.More() {
		var question Question
		if err := jsonDecoder.Decode(&question); err != nil {
			log.Fatalf("%v", err)
		}

		wg.Add(1)
		jobs <- question
	}

	// Read closing delimiter. `]` or `}`
	if _, err := jsonDecoder.Token(); err != nil {
		log.Fatalf("%v", err)
	}

	close(jobs)
}

func doTheJob(workerIndex int, counter int, db *gocb.Cluster, job Question) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			// Get a bucket reference
			bucket := db.Bucket("stack-overflow-bucket")

			// Wait until bucket is ready
			err := bucket.WaitUntilReady(5*time.Second, nil)
			if err != nil {
				log.Fatalf("%v", err)
			}

			// Get a scope reference
			scope := bucket.Scope("stack-exchange-scope")

			// Get a collection reference
			collection := scope.Collection("question-record-collection")

			// Unmarshall question to JSON
			input := []byte(job.RawQuestion)
			var questionJSON interface{}
			if err := json.Unmarshal(input, &questionJSON); err != nil {
				log.Fatalf("%v", err)
			}

			// Create a key
			key := fmt.Sprintf("question_%d", job.ID)

			// Insert document
			_, err = collection.Insert(key, questionJSON, nil)
			if err != nil {
				log.Fatalf("%v", err)
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

func dispatchWorkers(db *gocb.Cluster, jobs <-chan Question, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex < totalWorker; workerIndex++ {
		go func(workerIndex int, db *gocb.Cluster, jobs <-chan Question, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func main() {
	start := time.Now()

	db, err := openDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	jsonDecoder, jsonFile, err := openJSONFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer jsonFile.Close()

	jobs := make(chan Question, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readJSONFilePerObjectThenSendToWorker(jsonDecoder, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}
