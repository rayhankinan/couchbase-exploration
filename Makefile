PWD = $(shell pwd)

migrate-sql:
	psql "stack-exchange" < $(PWD)/sql/schema.sql

seed-sql:
	go run ./postgres/main.go

seed-no-sql:
	go run ./couchbase/main.go