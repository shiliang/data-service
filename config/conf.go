/*
*

	@author: shiliang
	@date: 2024/9/11
	@note: 读取k8s configmap的配置

*
*/
package config

import (
	"context"
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
)

type DataServiceConf struct {
	IDAServerAddress string `json:"IDAServerAddress"`
	IDAServerPort    string `json:"IDAServerPort"`
	OSSType          string `json:"OSSType"`
	OSSEndpoint      string `json:"OSSEndpoint"`
	AccessKeyID      string `json:"AccessKeyID"`
	SecretAccessKey  string `json:"SecretAccessKey"`
	UseSSL           bool   `json:"UseSSL"`
}

func parseConfigMap(data map[string]string) *DataServiceConf {
	config := &DataServiceConf{}
	configData, ok := data["config.json"]
	if ok {
		err := json.Unmarshal([]byte(configData), &config)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		if idaServer, ok := data["IDAServerAddress"]; ok {
			config.IDAServerAddress = idaServer
		}
		if idaServerPort, ok := data["IDAServerPort"]; ok {
			config.IDAServerPort = idaServerPort
		}
	}
	return config
}

/**
 * @Description 拿取数据服务configmap信息
 * @Param
 * @return configMap
 **/
func GetConfigMap() *DataServiceConf {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// 创建客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// 获取 ConfigMap
	configMap, err := clientset.CoreV1().ConfigMaps("your-namespace").Get(context.TODO(), "your-configmap-name", metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	return parseConfigMap(configMap.Data)
}
