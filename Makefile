GOLANG_CI_LINT_VERSION  ?= v2.11.4

.PHONY: build test lintcheck vulncheck clean fmt license-add license-check

build:
	go build -o tcp-pep-daemon cmd/main.go

test:
	go test -v -race ./...

fmt:
	@if ! command -v goimports >/dev/null 2>&1; then \
		echo "Installing goimports..."; \
		go install golang.org/x/tools/cmd/goimports@latest; \
	fi
	goimports -w .
	gofmt -s -w .

license-add:
	@if ! command -v addlicense >/dev/null 2>&1; then \
		echo "Installing addlicense..."; \
		go install github.com/google/addlicense@latest; \
	fi
	find . -name "*.go" -not -path "./vendor/*" | xargs addlicense -l apache -c "The tcp-pep-go Authors"

license-check:
	@echo "Checking for Apache 2.0 license headers..."
	@exit_code=0; \
	for file in $$(find . -name "*.go" -not -path "./vendor/*"); do \
		if ! head -n 20 $$file | grep -q "Apache License, Version 2.0"; then \
			echo "Missing license header in $$file"; \
			exit_code=1; \
		fi; \
	done; \
	exit $$exit_code

lintcheck: fmt
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		curl -sSfL https://raw.githubusercontent.com/golangci-lint/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.61.0; \
	fi
	golangci-lint run ./...

vulncheck:
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "Installing govulncheck..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
	fi
	govulncheck ./...

clean:
	rm -f tcp-pep-daemon
