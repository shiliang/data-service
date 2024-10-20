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
	"data-service/config"
	pb "data-service/generated/datasource"
	log "data-service/log"
	"fmt"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"time"
)

func CreateSparkPod(clientset *kubernetes.Clientset, podName string, info *pb.SparkDBConnInfo) (*v1.Pod, error) {
	conf := config.GetConfigMap()
	imageFullName := conf.SparkPodConfig.ImageName + ":" + conf.SparkPodConfig.ImageTag
	minioEndpoint := fmt.Sprintf("%s:%d", conf.OSSConfig.Host, conf.OSSConfig.Port)
	minioAccessKey := conf.OSSConfig.AccessKey
	minioSecretKey := conf.OSSConfig.SecretKey
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: conf.SparkPodConfig.Namespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: conf.SparkPodConfig.AccountName,
			Containers: []corev1.Container{
				{
					Name:  "spark-container",
					Image: imageFullName, // 替换为实际的 Spark 镜像
					Args: []string{
						"/opt/spark/bin/spark-submit",
						"--master", conf.SparkPodConfig.Master,
						"--deploy-mode", "cluster",
						"--conf", "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3",
						"--conf", fmt.Sprintf("spark.executor.instances=2"),
						"--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", conf.SparkPodConfig.Namespace),
						"--conf", fmt.Sprintf("spark.kubernetes.driver.container.image=%s", imageFullName),
						"--conf", fmt.Sprintf("spark.kubernetes.executor.container.image=%s", imageFullName),
						"--conf", fmt.Sprintf("spark.kubernetes.authenticate.driver.serviceAccountName=%s", conf.SparkPodConfig.AccountName), // Driver的ServiceAccount
						"--conf", fmt.Sprintf("spark.kubernetes.authenticate.executor.serviceAccountName=%s", conf.SparkPodConfig.AccountName), // Executor的ServiceAccount
						"/opt/spark/jars/spark-job.zip/__main__.py",
						"--dbtype", info.DbType,
						"--host", info.Host,
						"--port", fmt.Sprintf("%d", info.Port),
						"--database", info.Database,
						"--username", info.Username,
						"--password", info.Password,
						"--query", info.Query,
						"--serverip", conf.HttpServiceConfig.DataServer,
						"--serverport", fmt.Sprintf("%d", conf.HttpServiceConfig.DataServerPort),
						"--endpoint", minioEndpoint,
						"--accesskey", minioAccessKey,
						"--secretkey", minioSecretKey,
						"--filename", info.FileName,
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "spark-port",
							ContainerPort: 7077,
						},
					},
				},
			},
			Volumes:       []corev1.Volume{},
			RestartPolicy: corev1.RestartPolicyNever,
			DNSPolicy:     corev1.DNSClusterFirst,
			DNSConfig: &corev1.PodDNSConfig{
				Searches: []string{},
			},
		},
	}

	return clientset.CoreV1().Pods(conf.SparkPodConfig.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
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
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Logger.Fatalf("Error getting Kubernetes config: %v", err)
	}
	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		log.Logger.Fatalf("Error creating Kubernetes client: %v", err)
	}

	createSparkserviceAccount(clientset)
	conf := config.GetConfigMap()
	// 检查并创建 ClusterRole
	if !clusterRoleExists(clientset, conf.SparkPodConfig.ClusterRole) {
		createClusterRole(clientset, conf.SparkPodConfig.ClusterRole)
	} else {
		log.Logger.Warnf("ClusterRole '%s' already exists", conf.SparkPodConfig.ClusterRole)
	}

	// 检查并创建 ClusterRoleBinding
	if !clusterRoleBindingExists(clientset, conf.SparkPodConfig.ClusterRoleBind) {
		createClusterRoleBinding(clientset, conf.SparkPodConfig.ClusterRole, conf.SparkPodConfig.ClusterRoleBind,
			conf.SparkPodConfig.UserName)
	} else {
		log.Logger.Warnf("ClusterRoleBinding '%s' already exists", conf.SparkPodConfig.ClusterRoleBind)
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
func createClusterRole(clientset *kubernetes.Clientset, name string) {
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
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

	log.Logger.Infof("ClusterRole '%s' created successfully", name)
}

// 创建 ClusterRoleBinding
func createClusterRoleBinding(clientset *kubernetes.Clientset, roleName string, roleBindingName string, userName string) {
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: roleBindingName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:     "User",
				Name:     userName, // 替换为实际用户名
				APIGroup: "rbac.authorization.k8s.io",
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     roleName, // 绑定上面创建的角色
			APIGroup: "rbac.authorization.k8s.io",
		},
	}

	_, err := clientset.RbacV1().ClusterRoleBindings().Create(context.TODO(), clusterRoleBinding, metav1.CreateOptions{})
	if err != nil {
		log.Logger.Fatalf("Error creating ClusterRoleBinding: %v", err)
	}

	log.Logger.Infof("ClusterRoleBinding '%s' created successfully", roleBindingName)
}

func createSparkserviceAccount(clientset *kubernetes.Clientset) {
	conf := config.GetConfigMap()

	// 检查服务账户是否存在
	_, err := clientset.CoreV1().ServiceAccounts(conf.SparkPodConfig.Namespace).Get(context.TODO(),
		conf.SparkPodConfig.AccountName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// 服务账户不存在，创建它
			serviceAccount := &v1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      conf.SparkPodConfig.AccountName,
					Namespace: conf.SparkPodConfig.Namespace,
				},
			}

			_, err := clientset.CoreV1().ServiceAccounts(conf.SparkPodConfig.Namespace).Create(context.TODO(), serviceAccount, metav1.CreateOptions{})
			if err != nil {
				log.Logger.Fatalf("Error creating ServiceAccount: %v", err)
			}

			log.Logger.Infof("ServiceAccount '%s' created successfully", conf.SparkPodConfig.AccountName)
		} else {
			// 其他错误
			log.Logger.Fatalf("Error checking ServiceAccount: %v", err)
		}
	} else {
		// 服务账户已存在
		log.Logger.Infof("ServiceAccount '%s' already exists", conf.SparkPodConfig.AccountName)
	}
}
