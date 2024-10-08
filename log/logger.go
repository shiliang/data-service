/*
*

	@author: shiliang
	@date: 2024/10/8
	@note:

*
*/
package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var Logger *zap.SugaredLogger

func InitLogger() {
	// 初始化 logger
	if err := os.MkdirAll("../logs", os.ModePerm); err != nil {
		zap.S().Fatalw("failed to create log directory", zap.Error(err))
	}
	// 配置 Zap 日志器
	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: true,
		Encoding:    "json",
		OutputPaths: []string{"stdout", "../logs/app.log"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "msg",
		},
	}

	logger, err := cfg.Build()
	if err != nil {
		zap.S().Fatalw("failed to build zap logger", zap.Error(err))
	}

	// 创建 SugaredLogger
	Logger = logger.Sugar()
}
