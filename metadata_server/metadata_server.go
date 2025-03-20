package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

// FileInfo 结构体：存储文件的基本信息和分块信息
type FileInfo struct {
	Name        string       `json:"name"`
	Size        int64        `json:"size"`
	Type        string       `json:"type"`
	Hash        string       `json:"hash"`
	SuperChunks []SuperChunk `json:"superChunks"`
}

// SuperChunk 结构体：表示一个超级块
type SuperChunk struct {
	FileHash             string   `json:"fileHash"`
	Chunks               []Chunk  `json:"chunks"`
	RepresentativeHashes []string `json:"representativeHashes"`
	FileType             string   `json:"fileType"`
	StorageURL           string   `json:"StorageURL"`
}

// Chunk 结构体：表示一个文件块
type Chunk struct {
	Offset      int64  `json:"offset"`
	Hash        string `json:"hash"`
	Data        []byte `json:"data"`        // 块的二进制数据
	IsDuplicate bool   `json:"isDuplicate"` // 块是否已存在
}

// // FileManifest 结构体：记录文件的超级块信息
// type FileManifest struct {
//     Name        string       `json:"name"`
//     SuperChunks []SuperChunk `json:"superChunks"`
// }

// StorageNode 结构体：记录存储服务器的负载信息和文件类型
type StorageNode struct {
	URL       string   `json:"url"`       // 存储服务器的 URL
	Load      int64    `json:"load"`      // 存储服务器的负载
	FileTypes []string `json:"fileTypes"` // 存储服务器上的文件类型
}

// MetadataServer 结构体：元数据服务器，负责文件索引和路由
type MetadataServer struct {
	FileHashIndex   map[string]FileInfo // 文件哈希索引文件信息
	FileNameIndex   map[string]string   // 文件名索引文件哈希，不同文件名可能映射到同一个文件内容
	StorageNodes    []StorageNode       // 存储服务器列表
	FileTypeToNodes map[string][]string // 文件类型到存储服务器的映射表
	mu              sync.Mutex          // 互斥锁
}

// NewMetadataServer 函数：初始化元数据服务器
func NewMetadataServer() *MetadataServer {
	return &MetadataServer{
		FileNameIndex:   make(map[string]string),
		FileHashIndex:   make(map[string]FileInfo),
		StorageNodes:    []StorageNode{},
		FileTypeToNodes: make(map[string][]string),
	}
}

// 根据文件信息检查是否重复，不重复则路由超块，返回含存储地址信息和重复块信息的FileInfo
func (m *MetadataServer) processFileInfo(w http.ResponseWriter, r *http.Request) {
	var fileInfo FileInfo
	json.NewDecoder(r.Body).Decode(&fileInfo)

	m.mu.Lock()
	if _, exists := m.FileHashIndex[fileInfo.Hash]; exists {
		m.mu.Unlock()
		// 文件重复，直接返回
		fileInfo.Name = "isDuplicate"
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fileInfo)
		return
	}

	// 创建文件清单
	m.FileHashIndex[fileInfo.Hash] = fileInfo
	m.FileNameIndex[fileInfo.Name] = fileInfo.Hash
	m.mu.Unlock()

	for i, superChunk := range fileInfo.SuperChunks {
		superChunkWithUrl := routeSuperChunk(m, superChunk)
		fileInfo.SuperChunks[i] = superChunkWithUrl
	}
	//更新文件超块信息
	m.mu.Lock()
	m.FileHashIndex[fileInfo.Hash] = fileInfo
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(fileInfo)
}

// 用于字符串数组去重
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// 发送超级块信息到存储服务器，根据负载均衡和相似度返回最佳节点和非重复块的哈希值
func routeSuperChunk(m *MetadataServer, superChunk SuperChunk) SuperChunk {

	matchCount := 0
	m.mu.Lock()
	var selectedNodeUrl string
	minNode := &m.StorageNodes[0]
	for i := range m.StorageNodes {
		if m.StorageNodes[i].Load < minNode.Load {
			minNode = &m.StorageNodes[i]
		}
	}
	// 根据文件类型找到候选存储服务器
	candidateNodesUrl := m.FileTypeToNodes[superChunk.FileType]
	m.mu.Unlock()

	if len(candidateNodesUrl) == 0 {
		// 如果没有存储该类型的服务器，选择负载最小的服务器
		selectedNodeUrl = minNode.URL
	} else {
		// 查询候选存储服务器，获取重复块的数量计算相似度（并发版）
		var (
			bestMatchNodeUrl    string
			candidateSuperChunk SuperChunk
			mutex               sync.Mutex
			wg                  sync.WaitGroup
		)

		jsonData, err := json.Marshal(superChunk)
		if err != nil {
			log.Printf("Failed to marshal hashes")
			return SuperChunk{}
		}

		for _, nodeURL := range candidateNodesUrl {
			wg.Add(1)
			go func(nodeURL string) {
				defer wg.Done()

				url := fmt.Sprintf("%s/checkSuperChunks", nodeURL)
				resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					log.Printf("Failed to check chunks from %s: %v", nodeURL, err)
					return
				}
				defer resp.Body.Close()

				var result struct {
					MatchCount int        `json:"matchCount"`
					SuperChunk SuperChunk `json:"superChunk"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					log.Printf("Failed to decode response from %s: %v", nodeURL, err)
					return
				}

				mutex.Lock()
				defer mutex.Unlock()
				if result.MatchCount > matchCount {
					matchCount = result.MatchCount
					candidateSuperChunk = result.SuperChunk
					bestMatchNodeUrl = nodeURL
				}
			}(nodeURL)
		}
		wg.Wait()

		if matchCount == 0 { // 相似度都为 0
			var minWeight int64
			var minLoadNodeUrl string
			isFirst := true
			m.mu.Lock()
			for _, nodeURL := range candidateNodesUrl { // 获取候选服务器中负载最小的节点
				for i := range m.StorageNodes {
					if m.StorageNodes[i].URL == nodeURL { //从StorageNodes中找候选服务器信息
						if isFirst {
							minWeight = m.StorageNodes[i].Load
							minLoadNodeUrl = nodeURL
							isFirst = false
						}
						if m.StorageNodes[i].Load < minWeight {
							minWeight = m.StorageNodes[i].Load
							minLoadNodeUrl = m.StorageNodes[i].URL
						}
					}
				}
			}
			m.mu.Unlock()
			if minNode.Load != 0 && float32(minWeight)+40/float32(minNode.Load) < 1.2 { // 不会破坏负载均衡
				selectedNodeUrl = minLoadNodeUrl
			} else { // 会破坏负载均衡
				selectedNodeUrl = minNode.URL
			}
		} else { // 相似度不为 0
			selectedNodeUrl = bestMatchNodeUrl
			superChunk = candidateSuperChunk
		}
	}

	superChunk.StorageURL = selectedNodeUrl

	m.mu.Lock()
	// 更新存储服务器负载和文件类型
	for i := range m.StorageNodes {
		if m.StorageNodes[i].URL == selectedNodeUrl {
			m.StorageNodes[i].Load += int64(len(superChunk.Chunks) - matchCount) //暂时按数据块的个数作为负载
			if !contains(m.StorageNodes[i].FileTypes, superChunk.FileType) {     //重复的类型就不插入了
				m.StorageNodes[i].FileTypes = append(m.StorageNodes[i].FileTypes, superChunk.FileType)
			}
			break
		}
	}
	// 更新文件类型到存储服务器的映射表
	if !contains(m.FileTypeToNodes[superChunk.FileType], selectedNodeUrl) {
		m.FileTypeToNodes[superChunk.FileType] = append(m.FileTypeToNodes[superChunk.FileType], selectedNodeUrl)
	}
	m.mu.Unlock()

	return superChunk
}

// handleRegister 函数：处理存储节点注册请求
func (m *MetadataServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	fmt.Println("收到存储节点注册请求")

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取请求体失败", http.StatusBadRequest)
		return
	}

	// 解析请求体
	var nodeInfo StorageNode
	if err := json.Unmarshal(body, &nodeInfo); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}

	// 检查 URL 是否为空
	if nodeInfo.URL == "" {
		http.Error(w, "节点 URL 缺失", http.StatusBadRequest)
		return
	}

	// 将节点信息存储到 StorageNodes 中
	m.mu.Lock()
	m.StorageNodes = append(m.StorageNodes, nodeInfo)
	m.mu.Unlock()

	fmt.Printf("存储节点注册成功: %+v\n", nodeInfo)
	fmt.Println("当前存储节点信息：")
	for _, node := range m.StorageNodes {
		fmt.Println(node.URL, node.Load, node.FileTypes)
	}
	fmt.Println("----------------------------")
	w.WriteHeader(http.StatusOK)
}

// clean 函数：清除元数据服务器本地的所有数据，并并发调用所有存储服务器的 clean 接口
func (m *MetadataServer) clean(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//存储服务器发来的清理亲求
	var requestBody map[string]string
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "failed to decode request body", http.StatusBadRequest)
		return
	}
	// 检查是否包含 node_url 参数
	var deleteIndex int
	deleteNodeURL := requestBody["node_url"]
	if deleteNodeURL == "" {
		// 不带 node_url 参数的情况：清理所有数据
		fmt.Println("Cleaning data for all nodes")
	} else {
		// 带 node_url 参数的情况：清理指定节点的数据
		fmt.Printf("Cleaning data from node: %s\n", deleteNodeURL)
	}

	// 清除本地元数据
	m.FileHashIndex = make(map[string]FileInfo)
	m.FileNameIndex = make(map[string]string)
	m.FileTypeToNodes = make(map[string][]string)

	// 并发调用所有存储服务器的 clean 接口
	var wg sync.WaitGroup
	for i, node := range m.StorageNodes {
		if node.URL == deleteNodeURL {
			deleteIndex = i
		}
		m.StorageNodes[i].Load = 0
		m.StorageNodes[i].FileTypes = []string{}
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/clean", nodeURL)
			resp, err := http.Post(url, "application/json", bytes.NewReader([]byte{}))
			if err != nil {
				log.Printf("调用存储服务器 %s 的 clean 接口失败: %v\n", nodeURL, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("存储服务器 %s 的 clean 接口返回错误: %s\n", nodeURL, resp.Status)
				return
			}
			log.Printf("存储服务器 %s 的数据已清除\n", nodeURL)
		}(node.URL)
	}
	wg.Wait()

	if deleteNodeURL != "" {
		// 带 node_url 参数的情况：清理指定节点的数据
		m.StorageNodes = append(m.StorageNodes[:deleteIndex], m.StorageNodes[deleteIndex+1:]...)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "元数据服务器和所有存储服务器的数据已清除")
	fmt.Println("当前存储节点信息：")
	for _, node := range m.StorageNodes {
		fmt.Println(node.URL, node.Load, node.FileTypes)
	}
	fmt.Println("----------------------------")
}

// getAllStorageInfo 函数：调用所有存储服务器的 getStorageSize 接口，并汇总信息
func (m *MetadataServer) getAllStorageInfo(w http.ResponseWriter, r *http.Request) {

	// 并发调用所有存储服务器的 getStorageSize 接口
	var wg sync.WaitGroup
	storageInfos := make([]StorageNode, len(m.StorageNodes))
	for i, node := range m.StorageNodes {
		wg.Add(1)
		go func(i int, nodeURL string) {
			defer wg.Done()

			// 调用存储服务器的 getStorageSize 接口
			url := fmt.Sprintf("%s/getStorageInfo", nodeURL)
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("调用存储服务器 %s 的 getStorageSize 接口失败: %v\n", nodeURL, err)
				return
			}
			defer resp.Body.Close()

			// 解析响应
			var result struct {
				TotalDataSize int64    `json:"totalDataSize"`
				FileTypes     []string `json:"fileTypes"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				log.Printf("解析存储服务器 %s 的响应失败: %v\n", nodeURL, err)
				return
			}
			m.mu.Lock()
			// 存储结果
			storageInfos[i] = StorageNode{
				URL:       nodeURL,
				Load:      result.TotalDataSize,
				FileTypes: result.FileTypes,
			}
			m.mu.Unlock()
			fmt.Println("存储节点信息：",storageInfos[i])
		}(i, node.URL)
	}
	wg.Wait()

	// 总文件大小信息
	var totalFilesSize int64
	for _, file := range m.FileHashIndex {
		totalFilesSize += file.Size
	}

	// 返回汇总结果
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(struct {
		TotalFileSize int64         `json:"totalFilesSize"`
		Nodes         []StorageNode `json:"nodes"`
	}{
		TotalFileSize: totalFilesSize,
		Nodes:         storageInfos,
	})

	fmt.Println("当前存储节点信息：")
	fmt.Println("存储服务器数量：", len(m.StorageNodes))
	for _, node := range m.StorageNodes {
		fmt.Println(node.URL, node.Load, node.FileTypes)
	}
	fmt.Println("----------------------------")
}

// restoreFile 函数：处理文件恢复请求
func (m *MetadataServer) restoreFile(w http.ResponseWriter, r *http.Request) {
	// 解析请求体
	var requestBody struct {
		FileName string `json:"fileName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 查找文件哈希
	fileHash, exists := m.FileNameIndex[requestBody.FileName]
	if !exists {
		http.Error(w, "文件不存在", http.StatusNotFound)
		return
	}

	// 查找文件清单
	fileInfo, exists := m.FileHashIndex[fileHash]
	if !exists {
		http.Error(w, "文件元数据不存在", http.StatusNotFound)
		return
	}

	// 返回文件的超级块信息
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(fileInfo)
}

func main() {
	metadataServer := NewMetadataServer()

	// 注册路由
	http.HandleFunc("/processFileInfo", metadataServer.processFileInfo)
	http.HandleFunc("/register", metadataServer.handleRegister)
	http.HandleFunc("/getAllStorageInfo", metadataServer.getAllStorageInfo)
	http.HandleFunc("/clean", metadataServer.clean)
	http.HandleFunc("/restoreFile", metadataServer.restoreFile)
	// 启动元数据服务器
	fmt.Println("元数据服务器运行在端口 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
