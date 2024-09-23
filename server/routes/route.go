/*
*

	@author: shiliang
	@date: 2024/9/23
	@note:

*
*/
package routes

import "net/http"

// 定义一个全局通道用于传递 minioUrl
var MinioUrlChan = make(chan string)

// jobCompletionHandler 处理来自客户端的作业完成请求
func jobCompletionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// 从请求体中获取 MinIO URL
		var minioUrl string
		// 假设你已经解析了请求体并获得了 MinIO URL
		minioUrl = r.FormValue("filePath")
		MinioUrlChan <- minioUrl
		// 在这里可以进行文件处理的逻辑
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Job completed and MinIO URL received: " + minioUrl))
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

// RegisterRoutes 注册所有的 HTTP 路由
func RegisterRoutes() {
	http.HandleFunc("/api/job/completed", jobCompletionHandler)
}
