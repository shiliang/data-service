/*
*

	@author: shiliang
	@date: 2024/9/11
	@note: 读取k8s configmap的配置

*
*/
package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type DataServiceConf struct {
	OSSConfig         OSSConfig         `yaml:"oss"`
	Dbms              DbmsConfig        `yaml:"dbms"`
	HttpServiceConfig HttpServiceConfig `yaml:"http"`
	SparkPodConfig    sparkPodConfig    `yaml:"spark"`
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
	Port           int32  `yaml:"port"`
	DataServer     string `yaml:"data_server"`
	DataServerPort int32  `yaml:"data_server_port"`
}

type sparkPodConfig struct {
	AccountName     string `yaml:"account_name"`
	Namespace       string `yaml:"namespace"`
	UserName        string `yaml:"user_name"`
	ClusterRole     string `yaml:"cluster_role"`
	ClusterRoleBind string `yaml:"cluster_role_bind"`
	ImageName       string `yaml:"image_name"`
	ImageTag        string `yaml:"image_tag"`
	Class           string `yaml:"class"`
	Master          string `yaml:"master"`
	MinioJarPath    string `yaml:"minio_jar_path"`
}

func parseConfigMap() *DataServiceConf {
	config := &DataServiceConf{}
	configData, err := ioutil.ReadFile("../config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// 使用 yaml.Unmarshal 解析 YAML
	err = yaml.Unmarshal(configData, config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}

/**
 * @Description 拿取数据服务configmap信息
 * @Param
 * @return configMap
 **/
func GetConfigMap() *DataServiceConf {
	return parseConfigMap()
}
