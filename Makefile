BINARY     := go-exec
INSTALL    := /usr/local/bin/$(BINARY)
SERVICE    := /etc/systemd/system/$(BINARY).service
CONFIG     := /etc/go-exec

VERSION    := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

LDFLAGS := -s -w \
	-X main.Version=$(VERSION) \
	-X main.BuildTime=$(BUILD_TIME) \
	-X main.GitCommit=$(GIT_COMMIT)

.PHONY: all build install uninstall clean run

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) .

install: build
	@echo "[*] Installing $(INSTALL)"
	install -m 0755 $(BINARY) $(INSTALL)

	@echo "[*] Installing systemd service $(SERVICE)"
	install -m 0644 go-exec.service $(SERVICE)

	@echo "[*] Creating config $(CONFIG)"
	@if [ ! -f $(CONFIG) ]; then \
		install -m 0644 go-exec.example $(CONFIG); \
	else \
		echo "    $(CONFIG) already exists — skipping"; \
	fi

	systemctl daemon-reload
	systemctl enable $(BINARY)
	systemctl restart $(BINARY)
	@echo "[OK] Installed. systemctl status $(BINARY)"

uninstall:
	systemctl stop    $(BINARY) || true
	systemctl disable $(BINARY) || true
	rm -f $(INSTALL) $(SERVICE)
	systemctl daemon-reload
	@echo "[OK] Uninstalled."

clean:
	rm -f $(BINARY)

run: build
	./$(BINARY) -level debug -format text -config go-exec.example
