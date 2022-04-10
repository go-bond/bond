SHELL            = bash -o pipefail
TEST_FLAGS       ?= -p 1 -v


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

clean:
	go clean -cache -testcache

test: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) ./tests/...

test-all: test-clean
	GOGC=off go test $(TEST_FLAGS) $(MOD_VENDOR) -run=$(TEST) ./...

test-with-reset: db-reset test-all

test-clean:
	GOGC=off go clean -testcache

todo:
	@git grep TODO -- './*' ':!./vendor/' ':!./Makefile' || :

