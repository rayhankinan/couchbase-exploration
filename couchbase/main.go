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

type Job struct {
	RawQuestion string `json:"question"`
}

type RawQuestionJSON struct {
	ID           uint            `json:"id"`
	OwnerUserID  uint            `json:"owneruserid"`
	CreationDate string          `json:"creationdate"`
	Score        int             `json:"score"`
	Title        string          `json:"title"`
	Body         string          `json:"body"`
	RawAnswer    []RawAnswerJSON `json:"answers"`
	Tags         []string        `json:"tags"`
}

type RawAnswerJSON struct {
	ID           uint   `json:"id"`
	OwnerUserID  uint   `json:"owneruserid"`
	ParentID     uint   `json:"parentid"`
	CreationDate string `json:"creationdate"`
	Score        int    `json:"score"`
	Body         string `json:"body"`
}

type QuestionJSON struct {
	OwnerUserID  uint     `json:"owneruserid"`
	CreationDate string   `json:"creationdate"`
	Score        int      `json:"score"`
	Title        string   `json:"title"`
	Body         string   `json:"body"`
	AnswerIds    []uint   `json:"answerids"`
	Tags         []string `json:"tags"`
}

type AnswerJSON struct {
	OwnerUserID  uint   `json:"owneruserid"`
	ParentID     uint   `json:"parentid"`
	CreationDate string `json:"creationdate"`
	Score        int    `json:"score"`
	Body         string `json:"body"`
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

func readJSONFilePerObjectThenSendToWorker(jsonDecoder *json.Decoder, jobs chan<- Job, wg *sync.WaitGroup) {
	// Read opening delimiter. `[` or `{`
	if _, err := jsonDecoder.Token(); err != nil {
		log.Fatalf("%v", err)
	}

	// Read file content as long as there is something.
	for jsonDecoder.More() {
		var job Job
		if err := jsonDecoder.Decode(&job); err != nil {
			log.Fatalf("%v", err)
		}

		wg.Add(1)
		jobs <- job
	}

	// Read closing delimiter. `]` or `}`
	if _, err := jsonDecoder.Token(); err != nil {
		log.Fatalf("%v", err)
	}

	close(jobs)
}

func doTheJob(workerIndex int, counter int, db *gocb.Cluster, job Job) {
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

			// Get the question collection reference
			questionCollection := scope.Collection("question-record-collection")

			// Unmarshall question to JSON
			input := []byte(job.RawQuestion)
			rawQuestionJSON := RawQuestionJSON{}
			if err := json.Unmarshal(input, &rawQuestionJSON); err != nil {
				log.Fatalf("%v", err)
			}

			// Create question document key
			questionKey := fmt.Sprintf("question::%d", rawQuestionJSON.ID)

			// Create question document value
			answerIds := []uint{}
			for _, rawAnswer := range rawQuestionJSON.RawAnswer {
				answerIds = append(answerIds, rawAnswer.ID)
			}

			questionJSON := QuestionJSON{
				OwnerUserID:  rawQuestionJSON.OwnerUserID,
				CreationDate: rawQuestionJSON.CreationDate,
				Score:        rawQuestionJSON.Score,
				Title:        rawQuestionJSON.Title,
				Body:         rawQuestionJSON.Body,
				AnswerIds:    answerIds,
				Tags:         rawQuestionJSON.Tags,
			}

			// Insert question document
			_, err = questionCollection.Insert(questionKey, questionJSON, nil)
			if err != nil {
				log.Fatalf("%v", err)
			}

			// Get the answer collection reference
			answerCollection := scope.Collection("answer-record-collection")

			// For each answer, insert answer document
			for _, rawAnswer := range rawQuestionJSON.RawAnswer {
				// Create answer document key
				answerKey := fmt.Sprintf("answer::%d", rawAnswer.ID)

				// Create answer document value
				answerJSON := AnswerJSON{
					OwnerUserID:  rawAnswer.OwnerUserID,
					ParentID:     rawAnswer.ParentID,
					CreationDate: rawAnswer.CreationDate,
					Score:        rawAnswer.Score,
					Body:         rawAnswer.Body,
				}

				// Insert answer document
				_, err = answerCollection.Insert(answerKey, answerJSON, nil)
				if err != nil {
					log.Fatalf("%v", err)
				}
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

func dispatchWorkers(db *gocb.Cluster, jobs <-chan Job, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex < totalWorker; workerIndex++ {
		go func(workerIndex int, db *gocb.Cluster, jobs <-chan Job, wg *sync.WaitGroup) {
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

	jobs := make(chan Job, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readJSONFilePerObjectThenSendToWorker(jsonDecoder, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}
