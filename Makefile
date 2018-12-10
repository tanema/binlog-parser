install:
	@go install ./cmd/binlog-parser

test:
	@go test ./...

unit-test:
	@go test -cover ./src/...

integration-test:
	@go test ./test/...
