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
	"gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"os"
)

type DataServiceConf struct {
	OSSConfig         OSSConfig         `yaml:"oss"`
	Dbms              DbmsConfig        `yaml:"dbms"`
	HttpServiceConfig HttpServiceConfig `yaml:"http"`
}

type DbmsConfig struct {
	Type         string `yaml:"type"`
	Params       string `yaml:"params"`
	Host         string `yaml:"host"`
	Port         int32  `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Database     string `yaml:"db"`
	dsn          string `yaml:"dsn"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

type OSSConfig struct {
	Type      string `yaml:"type"`
	Host      string `yaml:"host"`
	Port      int32  `yaml:"port"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type HttpServiceConfig struct {
	Port int32 `yaml:"port"`
}

func parseConfigMap(data map[string]string) *DataServiceConf {
	config := &DataServiceConf{}
	// 获取 YAML 数据
	configData, ok := data["config.yaml"]
	if ok {
		// 使用 yaml.Unmarshal 解析 YAML
		err := yaml.Unmarshal([]byte(configData), config)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("config.yaml not found in the config map")
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
	namespace := os.Getenv("POD_NAMESPACE")
	configMapName := "mira-data-service-config"
	configMap, err := clientset.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	return parseConfigMap(configMap.Data)
}
