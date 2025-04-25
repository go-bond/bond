SHELL            = bash -o pipefail
TEST_FLAGS       ?= -p 1 -v -race -failfast

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo ""
	@echo " + Development:"
	@echo "   - build"
	@echo "   - test"
	@echo "   - todo"
	@echo "   - clean"
	@echo ""
	@echo ""
	@echo " + Database stuff:"
	@echo "   - db-reset"
	@echo "   - db-create"
	@echo "   - db-drop"
	@echo ""


##
## Development
##
build:
	go build ./...

clean: test-clean
	go clean -cache -testcache

test: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) ./...

test-all: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) ./...

test-with-reset: db-reset test-all

test-clean: db-reset
	GOGC=off go clean -testcache

bench: clean
	@cd _benchmarks && go test -timeout=25m -bench=.

bench-csv: clean
	@cd _benchmarks && go run ./benchmark.go --report=csv

todo:
	@git grep TODO -- './*' ':!./vendor/' ':!./Makefile' || :

db-reset:
	rm -rf test_db tmp_db _benchmarks/bench_db
