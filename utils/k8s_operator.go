/*
*

	@author: shiliang
	@date: 2024/9/23
	@note: k8s的相关操作

*
*/
package utils

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"time"
)

func CreateSparkPod(clientset *kubernetes.Clientset, namespace string, podName string, jdbcUrl string, tlsCert string) (*v1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spark-container",
					Image: "spark:3.5.2", // 替换为实际的 Spark 镜像
					Args: []string{
						"/opt/spark/bin/spark-submit",
						"--class", "com.chainmaker.DynamicDatabaseJob",
						"--master", "k8s://https://kubernetes.default.svc",
						"--deploy-mode", "cluster",
						"--files", tlsCert,
						"--conf", fmt.Sprintf("spark.executor.instances=2"),
						"--conf", fmt.Sprintf("spark.datasource.jdbc.url=%s", jdbcUrl),
						"local:///opt/spark/jars/spark-scala-app-1.0-SNAPSHOT-jar-with-dependencies.jar",
					},
					Env: []corev1.EnvVar{
						{
							Name:  "JDBC_URL",
							Value: jdbcUrl,
						},
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "spark-port",
							ContainerPort: 7077,
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	return clientset.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
}

// generatePodName 生成具有唯一性的 Pod 名称。
// 通过组合基础名称、当前时间戳和一个唯一标识符 (UUID) 的前 8 位来确保名称的唯一性。
// 参数:
//
//	baseName - 用于生成 Pod 名称的基础名称。
//
// 返回值:
//
//	格式化后的具有唯一性的 Pod 名称。
func generatePodName(baseName string) string {
	// 获取当前时间并格式化为字符串，格式为：20060102-150405
	timestamp := time.Now().Format("20060102-150405")
	// 生成一个 UUID 并获取其前 8 位作为唯一标识符
	uniqueID := uuid.New().String()[:8]
	// 返回格式化后的 Pod 名称
	return fmt.Sprintf("%s-%s-%s", baseName, timestamp, uniqueID)
}

func monitorPod(clientset *kubernetes.Clientset, namespace, podName string, callback func(string)) {
	for {
		pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			log.Fatalf("Error getting Pod: %s", err.Error())
		}

		fmt.Printf("Current Pod phase: %s\n", pod.Status.Phase)

		if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			callback(string(pod.Status.Phase))
			break
		}

		time.Sleep(10 * time.Second) // 每隔 10 秒检查一次
	}
}
