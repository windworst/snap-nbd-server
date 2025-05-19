.PHONY: all clean server

# 编译器和编译选项
GO=go
GOFLAGS=-v -buildvcs=false
LDFLAGS=-w -s

# 目标程序
SERVER=snap-nbd.exe

all: server

server:
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(SERVER) .

clean:
	rm -f $(SERVER)

# 安装依赖
deps:
	$(GO) mod download
	$(GO) mod tidy

# 运行测试
test:
	$(GO) test -v ./...

# 帮助信息
help:
	@echo "可用的目标:"
	@echo "  all      - 编译服务器程序"
	@echo "  server   - 编译服务器程序"
	@echo "  clean    - 清理编译产物"
	@echo "  deps     - 下载依赖"
	@echo "  test     - 运行测试"
