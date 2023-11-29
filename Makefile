PWD = $(shell pwd)

migrate-sql:
	psql "stack-exchange" < $(PWD)/sql/schema.sql

seed-sql:
	go run ./sql/main.go

seed-no-sql:
	go run ./no-sql/main.go