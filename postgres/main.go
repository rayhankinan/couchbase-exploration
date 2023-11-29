package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"nosql-script-seeding/postgres/answer"
	"nosql-script-seeding/postgres/question"
	"nosql-script-seeding/postgres/tag"
)

func main() {
	start := time.Now()

	log.Println("=> seeding questions")
	question.Execute()

	log.Println("=> seeding answers")
	answer.Execute()

	log.Println("=> seeding tags")
	tag.Execute()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}
