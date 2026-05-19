MODULE  := github.com/supesu/faultmesh/data-plane
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X '$(MODULE)/pkg/version.Version=$(VERSION)' \
           -X '$(MODULE)/pkg/version.GitCommit=$(COMMIT)' \
           -X '$(MODULE)/pkg/version.BuildDate=$(DATE)'

CMDS := fm-agent fm-ctl fm-executor fm-operator
BIN  := bin

.PHONY: all build clean $(CMDS)

all: build

build: $(CMDS)

$(CMDS):
	go build -ldflags="$(LDFLAGS)" -o $(BIN)/$@ ./cmd/$@

clean:
	rm -rf $(BIN)
