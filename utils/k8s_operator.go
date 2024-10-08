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
	log "github.com/shiliang/data-service/log"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CreateSparkPod(clientset *kubernetes.Clientset, namespace string, podName string, jdbcUrl string) (*v1.Pod, error) {
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

func SetupKubernetesClientAndResources() {
	var kubeconfig string
	// 获取 home 目录
	if home := homedir.HomeDir(); home != "" {
		kubeDir := filepath.Join(home, ".kube")
		kubeconfig = filepath.Join(kubeDir, "config")

		// 检查并创建 .kube 目录（如果不存在）
		if _, err := os.Stat(kubeDir); os.IsNotExist(err) {
			err := os.MkdirAll(kubeDir, os.ModePerm) // 创建目录
			if err != nil {
				log.Logger.Errorf("Failed to create .kube directory: %v\n", err)
				return
			}
			log.Logger.Info(".kube directory created successfully")
		}
	}

	log.Logger.Info("Kubeconfig path:", kubeconfig)

	// 通过 kubeconfig 构建配置
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Logger.Fatalf("Error building kubeconfig: %v", err)
	}

	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Logger.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// 检查并创建 ClusterRole
	if !clusterRoleExists(clientset, "spark-user-role") {
		createClusterRole(clientset)
	} else {
		fmt.Println("ClusterRole 'spark-user-role' already exists")
	}

	// 检查并创建 ClusterRoleBinding
	if !clusterRoleBindingExists(clientset, "spark-user-rolebinding") {
		createClusterRoleBinding(clientset)
	} else {
		fmt.Println("ClusterRoleBinding 'spark-user-rolebinding' already exists")
	}
}

// 检查 ClusterRole 是否存在
func clusterRoleExists(clientset *kubernetes.Clientset, name string) bool {
	_, err := clientset.RbacV1().ClusterRoles().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false
		}
		log.Logger.Fatalf("Error checking ClusterRole: %v", err)
	}
	return true
}

// 检查 ClusterRoleBinding 是否存在
func clusterRoleBindingExists(clientset *kubernetes.Clientset, name string) bool {
	_, err := clientset.RbacV1().ClusterRoleBindings().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false
		}
		log.Logger.Fatalf("Error checking ClusterRoleBinding: %v", err)
	}
	return true
}

// 创建 ClusterRole
func createClusterRole(clientset *kubernetes.Clientset) {
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-user-role",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "services", "configmaps", "persistentvolumeclaims"},
				Verbs:     []string{"get", "list", "watch", "create", "delete"},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs"},
				Verbs:     []string{"get", "list", "watch", "create", "delete"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments"},
				Verbs:     []string{"get", "list", "watch", "create", "delete"},
			},
			{
				APIGroups: []string{"sparkoperator.k8s.io"},
				Resources: []string{"sparkapplications", "scheduledsparkapplications"},
				Verbs:     []string{"get", "list", "watch", "create", "delete"},
			},
		},
	}

	_, err := clientset.RbacV1().ClusterRoles().Create(context.TODO(), clusterRole, metav1.CreateOptions{})
	if err != nil {
		log.Logger.Fatalf("Error creating ClusterRole: %v", err)
	}

	log.Logger.Info("ClusterRole 'spark-user-role' created successfully")
}

// 创建 ClusterRoleBinding
func createClusterRoleBinding(clientset *kubernetes.Clientset) {
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-user-rolebinding",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:     "User",
				Name:     "spark-user", // 替换为实际用户名
				APIGroup: "rbac.authorization.k8s.io",
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "spark-user-role", // 绑定上面创建的角色
			APIGroup: "rbac.authorization.k8s.io",
		},
	}

	_, err := clientset.RbacV1().ClusterRoleBindings().Create(context.TODO(), clusterRoleBinding, metav1.CreateOptions{})
	if err != nil {
		log.Logger.Fatalf("Error creating ClusterRoleBinding: %v", err)
	}

	fmt.Println("ClusterRoleBinding 'spark-user-rolebinding' created successfully")
}
