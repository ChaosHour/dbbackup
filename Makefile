# Makefile for dbbackup
# Provides common development workflows

.PHONY: build test lint vet clean install-tools help race cover golangci-lint

# Build variables
VERSION := $(shell grep 'version.*=' main.go | head -1 | sed 's/.*"\(.*\)".*/\1/')
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S_UTC')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -w -s -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.gitCommit=$(GIT_COMMIT)

# Default target
all: lint test build

## build: Build the binary with optimizations
build:
	@echo "🔨 Building dbbackup $(VERSION)..."
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o bin/dbbackup .
	@echo "✅ Built bin/dbbackup"

## build-debug: Build with debug symbols (for debugging)
build-debug:
	@echo "🔨 Building dbbackup $(VERSION) with debug symbols..."
	go build -ldflags="-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.gitCommit=$(GIT_COMMIT)" -o bin/dbbackup-debug .
	@echo "✅ Built bin/dbbackup-debug"

## test: Run tests
test:
	@echo "🧪 Running tests..."
	go test ./...

## race: Run tests with race detector
race:
	@echo "🏃 Running tests with race detector..."
	go test -race ./...

## cover: Run tests with coverage report
cover:
	@echo "📊 Running tests with coverage..."
	go test -cover ./... | tee coverage.txt
	@echo "📄 Coverage saved to coverage.txt"

## cover-html: Generate HTML coverage report
cover-html:
	@echo "📊 Generating HTML coverage report..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "📄 Coverage report: coverage.html"

## lint: Run all linters
lint: vet staticcheck golangci-lint

## vet: Run go vet
vet:
	@echo "🔍 Running go vet..."
	go vet ./...

## staticcheck: Run staticcheck (install if missing)
staticcheck:
	@echo "🔍 Running staticcheck..."
	@if ! command -v staticcheck >/dev/null 2>&1; then \
		echo "Installing staticcheck..."; \
		go install honnef.co/go/tools/cmd/staticcheck@latest; \
	fi
	$$(go env GOPATH)/bin/staticcheck ./...

## golangci-lint: Run golangci-lint (comprehensive linting)
golangci-lint:
	@echo "🔍 Running golangci-lint..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	$$(go env GOPATH)/bin/golangci-lint run --timeout 5m

## install-tools: Install development tools
install-tools:
	@echo "📦 Installing development tools..."
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "✅ Tools installed"

## fmt: Format code
fmt:
	@echo "🎨 Formatting code..."
	gofmt -w -s .
	@which goimports > /dev/null && goimports -w . || true

## tidy: Tidy and verify go.mod
tidy:
	@echo "🧹 Tidying go.mod..."
	go mod tidy
	go mod verify

## update: Update dependencies
update:
	@echo "⬆️  Updating dependencies..."
	go get -u ./...
	go mod tidy

## clean: Clean build artifacts
clean:
	@echo "🧹 Cleaning..."
	rm -rf bin/dbbackup bin/dbbackup-debug
	rm -f coverage.out coverage.txt coverage.html
	go clean -cache -testcache

## docker: Build Docker image
docker:
	@echo "🐳 Building Docker image..."
	docker build -t dbbackup:$(VERSION) .

## all-platforms: Build for all platforms (uses build_all.sh)
all-platforms:
	@echo "🌍 Building for all platforms..."
	./build_all.sh

## help: Show this help
help:
	@echo "dbbackup Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'
