package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var (
	dataNodePort      string                        // 存储节点的端口号
	storageDir        = "./data_blocks"             // 存储块的目录
	metadataServerURL = "http://192.168.1.108:8080" // 元数据服务器的 URL
	localURL          string                        // 本机 URL
)

// getWLANIP 获取本机的 IP 地址（支持无线网络和有线网络）
func getWLANIP() (string, error) {
	// 获取所有网络接口
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("failed to get network interfaces: %v", err)
	}

	// 遍历所有接口
	for _, iface := range interfaces {
		// 跳过回环接口和未启用的接口
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// 检查接口名称，支持无线网络和以太网
		ifaceName := iface.Name
		if strings.Contains(ifaceName, "WLAN") || strings.Contains(ifaceName, "以太网") {
			// 获取接口地址
			addrs, err := iface.Addrs()
			if err != nil {
				fmt.Printf("Failed to get addresses for interface %s: %v\n", iface.Name, err)
				continue
			}

			// 遍历地址，找到 IPv4 地址
			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					fmt.Printf("本机 IP 地址为：%s (接口：%s)\n", ipNet.IP.String(), iface.Name)
					return ipNet.IP.String(), nil
				}
			}
		}
	}

	return "", fmt.Errorf("no valid IPv4 address found for WLAN or Ethernet interface")
}

// 获取一个可用端口号
func getAvailablePort() (int, error) {
	// 监听一个随机端口
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	// 获取监听的端口号
	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

// 计算当前存储的块总大小
func calculateTotalStorageSize() (int64, error) {
	// 检查存储目录是否存在
	if _, err := os.Stat(storageDir); os.IsNotExist(err) {
		// 如果目录不存在，返回 0
		return 0, nil
	}

	var totalSize int64
	err := filepath.Walk(storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("计算存储大小失败: %v", err)
	}
	return totalSize, nil
}

// 向元数据服务器注册自己
func registerWithMetadataServer() error {
	// 获取 WLAN 接口的 IP 地址
	ip, err := getWLANIP()
	if err != nil {
		return fmt.Errorf("获取 WLAN IP 失败: %v", err)
	}

	// 获取可用端口号
	port, err := getAvailablePort()
	if err != nil {
		return fmt.Errorf("获取可用端口号失败: %v", err)
	}
	dataNodePort = ":" + strconv.Itoa(port)
	storageDir = storageDir + "/" + strconv.Itoa(port) + "端口"

	// 创建存储目录
	if err := os.MkdirAll(storageDir, os.ModePerm); err != nil {
		return fmt.Errorf("创建存储目录失败: %v", err)
	}

	// 计算当前存储的块总大小
	totalSize, err := calculateTotalStorageSize()
	if err != nil {
		return fmt.Errorf("计算存储大小失败: %v", err)
	}

	// 构造注册请求
	localURL = "http://" + ip + dataNodePort
	nodeInfo := map[string]interface{}{
		"url":      localURL, // 数据节点的 URL
		"dataSize": totalSize,
	}
	jsonData, err := json.Marshal(nodeInfo)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %v", err)
	}

	// 发送注册请求
	req, err := http.NewRequest("POST", metadataServerURL+"/register", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("创建注册请求失败: %v", err)
	}
	req.Header.Set("Node-URL", "http://"+ip+dataNodePort)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送注册请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("注册请求失败，状态码: %s", resp.Status)
	}

	fmt.Println("成功注册到元数据服务器")
	return nil
}

// StorageServer 结构体：存储服务器，负责存储块
type StorageServer struct {
	mu sync.Mutex // 互斥锁
}

// storeChunks 函数：将块存储到存储服务器
func (s *StorageServer) storeChunks(w http.ResponseWriter, r *http.Request) {
	var totalSize int64 = 0
	var requestBody struct {
		Chunks []struct {
			Offset int64  `json:"offset"`
			Hash   string `json:"hash"`
			Data   []byte `json:"data"`
		} `json:"chunks"`
		FileType string `json:"fileType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 将块存储到对应类型的文件夹中
	for _, chunk := range requestBody.Chunks {
		totalSize += int64(len(chunk.Data))
		dir := filepath.Join(storageDir, requestBody.FileType)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			log.Printf("创建目录 %s 失败: %v\n", dir, err)
			http.Error(w, "创建目录失败", http.StatusInternalServerError)
			return
		}

		filePath := filepath.Join(dir, chunk.Hash)
		if err := os.WriteFile(filePath, chunk.Data, 0644); err != nil {
			log.Printf("写入块 %s 失败: %v\n", filePath, err)
			http.Error(w, "写入块失败", http.StatusInternalServerError)
			return
		}
	}

	// 返回存储后的 data_blocks 大小
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(struct {
		TotalSize int64 `json:"totalSize"`
	}{
		TotalSize: totalSize,
	})
}

// checkChunks 函数：根据文件类型，在对应文件夹下检查哈希匹配数量，并返回重复的哈希值
func (s *StorageServer) checkChunks(w http.ResponseWriter, r *http.Request) {
	var requestBody struct {
		FileType string   `json:"fileType"`
		Hashes   []string `json:"hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 在对应文件类型的文件夹下检查哈希匹配
	var duplicateHashes []string
	dir := filepath.Join(storageDir, requestBody.FileType)
	for _, hash := range requestBody.Hashes {
		filePath := filepath.Join(dir, hash)
		if _, err := os.Stat(filePath); err == nil {
			duplicateHashes = append(duplicateHashes, hash)
		}
	}

	// 返回匹配的哈希数量和重复的哈希值
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(struct {
		MatchCount      int      `json:"matchCount"`
		DuplicateHashes []string `json:"duplicateHashes"`
	}{
		MatchCount:      len(duplicateHashes),
		DuplicateHashes: duplicateHashes,
	})
}

// clean 函数：清除本地存储的所有数据
func (s *StorageServer) clean(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 删除存储目录及其所有内容
	if err := os.RemoveAll("./data_blocks"); err != nil {
		http.Error(w, fmt.Sprintf("清除存储目录失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 重新创建存储目录
	if err := os.MkdirAll("./data_blocks", os.ModePerm); err != nil {
		http.Error(w, fmt.Sprintf("重新创建存储目录失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "存储数据已清除")
}

// getStorageInfo 函数：计算 storageDir 下存储的数据总大小和数据类型
func (s *StorageServer) getStorageInfo(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查存储目录是否存在
	if _, err := os.Stat(storageDir); os.IsNotExist(err) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			TotalSize int64    `json:"totalSize"`
			FileTypes []string `json:"fileTypes"`
		}{
			TotalSize: 0,
			FileTypes: []string{},
		})
		return
	}

	// 计算存储目录下所有文件的总大小和数据类型
	var totalSize int64
	fileTypes := make(map[string]bool) // 使用 map 去重

	err := filepath.Walk(storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果是文件夹，并且是直接子文件夹（例如 .png, .pptx, .txt）
		if info.IsDir() && path != storageDir {
			// 获取文件夹名称（例如 ".png"）
			folderName := filepath.Base(path)
			if folderName != "" {
				fileTypes[folderName] = true
			}
		}

		// 如果是文件，累加文件大小
		if !info.IsDir() {
			totalSize += info.Size()
		}

		return nil
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("计算存储大小失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 将 map 转换为 slice
	var types []string
	for fileType := range fileTypes {
		types = append(types, fileType)
	}

	// 返回总大小和数据类型
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(struct {
		TotalSize int64    `json:"totalSize"`
		FileTypes []string `json:"fileTypes"`
	}{
		TotalSize: totalSize,
		FileTypes: types,
	})
}

func cleanup() error {
	fmt.Println("执行清理操作...")

	// 构造请求体
	requestBody := map[string]string{
		"node_url": localURL, // 将本机 url 作为请求体的一部分
	}
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %v", err)
	}

	// 发送 HTTP POST 请求到 metadata_server 的 /clean 接口
	resp, err := http.Post(metadataServerURL+"/clean", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error sending clear request: %v", err)
	}
	defer resp.Body.Close()

	// 检查响应状态码
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clear request failed with status: %s", resp.Status)
	}

	fmt.Println("Metadata server data cleared successfully")
	return nil
}

// getChunk 函数：根据哈希值获取块数据
func (s *StorageServer) getChunk(w http.ResponseWriter, r *http.Request) {
    // 解析请求体
    var requestBody struct {
        Hash string `json:"hash"`
    }
    if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
        http.Error(w, "解析请求体失败", http.StatusBadRequest)
        return
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    // 遍历所有文件类型文件夹，查找块数据
    dirs, err := os.ReadDir(storageDir)
    if err != nil {
        http.Error(w, fmt.Sprintf("读取存储目录失败: %v", err), http.StatusInternalServerError)
        return
    }

    for _, dir := range dirs {
        if dir.IsDir() {
            filePath := filepath.Join(storageDir, dir.Name(), requestBody.Hash)
            if _, err := os.Stat(filePath); err == nil {
                // 读取块数据
                chunkData, err := os.ReadFile(filePath)
                if err != nil {
                    http.Error(w, fmt.Sprintf("读取块数据失败: %v", err), http.StatusInternalServerError)
                    return
                }

                // 返回块数据
                w.WriteHeader(http.StatusOK)
                w.Write(chunkData)
                return
            }
        }
    }

    // 如果未找到块数据，返回 404
    http.Error(w, "块数据不存在", http.StatusNotFound)
}

func main() {
	// 捕获退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动一个 goroutine 处理信号
	go func() {
		<-sigChan // 等待信号
		fmt.Println("程序退出中...")
		if err := cleanup(); err != nil {
			fmt.Println("清理失败:", err)
		}
		os.Exit(0) // 退出程序
	}()

	// 输入元数据服务器 IP 地址
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("请输入元数据服务器 IP 地址，输入为空则按默认ip(%s)连接：", metadataServerURL)
	ipInput, _ := reader.ReadString('\n')
	ipInput = strings.TrimSpace(ipInput) // 去除输入中的换行符和空格
	if ipInput != "" {
		// 检查 IP 地址的合法性
		if net.ParseIP(ipInput) != nil {
			// 如果合法，覆盖
			metadataServerURL = "http://" + ipInput + ":8080"
			fmt.Printf("输入的元数据服务器 IP 地址为: %s\n", metadataServerURL)
		} else {
			// 如果不合法，提示错误
			fmt.Println("输入的 IP 地址不合法")
			return
		}
	}

	// 注册存储节点
	if err := registerWithMetadataServer(); err != nil {
		fmt.Println("注册到元数据服务器失败:", err)
		os.Exit(1)
	}

	// 启动存储服务器
	storageServer := &StorageServer{}
	http.HandleFunc("/storeChunks", storageServer.storeChunks)
	http.HandleFunc("/checkChunks", storageServer.checkChunks)
	http.HandleFunc("/getStorageInfo", storageServer.getStorageInfo)
	http.HandleFunc("/clean", storageServer.clean)
    http.HandleFunc("/getChunk", storageServer.getChunk)

	fmt.Printf("存储服务器运行在端口 %s...\n", dataNodePort)
	log.Fatal(http.ListenAndServe(dataNodePort, nil))

}
