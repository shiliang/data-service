# **********编译程序镜像**********
# 使用ubuntu作为基础镜像
FROM ubuntu:latest as builder

# 更新包管理工具并安装curl、git和其他依赖
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 设置Go的版本
ENV GO_VERSION=1.23.0

# 下载并安装指定版本的Go
RUN curl -OL https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_VERSION}.linux-amd64.tar.gz

# 配置Go环境变量
ENV PATH $PATH:/usr/local/go/bin

# 设置工作目录
WORKDIR /home/workspace

# 配置Go依赖环境
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

# 拷贝项目源文件
ADD ./ /home/workspace

# 构建Go应用，并启用调试信息
RUN cd /home/workspace && go mod tidy && go build -gcflags="all=-N -l" -o dataserver ./server/server.go

# 安装 Delve 调试工具
RUN go install github.com/go-delve/delve/cmd/dlv@latest
RUN go env

# 准备输出文件夹
RUN mkdir -p /home/workspace/bin && mkdir -p /home/workspace/jars

# **********轻量级运行镜像**********
FROM ubuntu:latest

# 安装 Delve 调试工具
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 从builder阶段复制二进制文件和调试工具到最终镜像
COPY --from=builder /home/workspace/dataserver /home/workspace/bin/
COPY --from=builder /root/go/bin/dlv /usr/local/bin/
COPY --from=builder /home/workspace/jars/spark-scala-app-1.0-SNAPSHOT-jar-with-dependencies.jar /home/workspace/jars/
COPY --from=builder /home/workspace/start.sh /home/workspace/bin/

# 授予start.sh可执行权限
RUN chmod +x /home/workspace/bin/start.sh

CMD /home/workspace/bin/start.sh

