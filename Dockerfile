#**********编译程序镜像*********
#依赖的基础镜像
FROM golang:1.23.0 as builder

WORKDIR /home/workspace

#配置go依赖环境
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct

#拷贝项目源文件，并取出src路径；相对路径以Dockerfile所在位值为基础
ADD ./ /home/workspace

#RUN 执行指定的shell命令；每条RUN命令，当前路径都是以 WORKDIR 为基础
#build scheduler
RUN cd /home/workspace && go mod tidy && go build -o dataserver ./server/server.go

RUN mkdir -p /home/workspace/bin && mkdir -p /home/workspace/jars
# 使用轻量级镜像运行编译后的文件
#FROM alpine:latest
FROM ubuntu:latest
# 将编译的二进制文件从 builder 镜像复制到当前镜像
COPY --from=builder /home/workspace/dataserver /home/workspace/bin/
COPY --from=builder /home/workspace/jars/spark-scala-app-1.0-SNAPSHOT-jar-with-dependencies.jar /home/workspace/jars/
COPY --from=builder /home/workspace/start.sh /home/workspace/bin/
RUN chmod +x  /home/workspace/bin/start.sh
# 设置容器启动时运行的命令
CMD /home/workspace/bin/start.sh
