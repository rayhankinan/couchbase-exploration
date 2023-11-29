package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"nosql-script-seeding/sql/answer"
	"nosql-script-seeding/sql/question"
	"nosql-script-seeding/sql/tag"
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
