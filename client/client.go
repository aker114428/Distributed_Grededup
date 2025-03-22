package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jotfs/fastcdc-go"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	datasetDir = "./dataset" // 数据集文件夹路径
)

var (
	metadataServerURL         = "http://192.168.1.203:8080"
	absoluteDedupRatio        = 1.0 //绝对重删率
	oneNodeAbsoluteDedupRatio = 1.0 //单节点绝对重删率：重删前文件总大小除以重删后文件总大小
	relativeDedupRatio        = 1.0 //多节点相对重删率：多节点绝对重删率除以单节点系统的绝对重删率
	mu                        sync.Mutex
)

// 全局 HTTP 客户端，带超时与连接复用
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	},
}


// FileInfo 结构体：存储文件的基本信息和分块信息
type FileInfo struct {
	Name        string       `json:"name"`        // 文件名
	Size        int64        `json:"size"`        // 文件大小
	Type        string       `json:"type"`        // 文件类型
	Hash        string       `json:"hash"`        // 文件哈希值
	SuperChunks []SuperChunk `json:"superChunks"` // 文件的超级块列表
}

// SuperChunk 结构体：表示一个超级块
type SuperChunk struct {
	FileHash             string   `json:"fileHash"`             //该超级块归属于哪个文件（用hash表示）
	Chunks               []Chunk  `json:"chunks"`               // 超级块中包含的块列表
	RepresentativeHashes []string `json:"representativeHashes"` // 超级块的代表性哈希值列表
	FileType             string   `json:"fileType"`             // 文件类型
	StorageURL           string   `json:"StorageURL"`
}

// Chunk 结构体：表示一个文件块
type Chunk struct {
	Offset      int64  `json:"offset"`      // 块在文件中的偏移量
	Hash        string `json:"hash"`        // 块的哈希值
	Data        []byte `json:"data"`        // 块的二进制数据
	IsDuplicate bool   `json:"isDuplicate"` // 块是否已存在
}

// StorageInfo 结构体：存储服务器的存储信息
type StorageInfo struct {
	URL       string   `json:"url"`
	Load      int64    `json:"load"`
	FileTypes []string `json:"fileTypes"`
}

// computeHash 函数：计算数据的 MD5 哈希值
func computeHash(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// hexToInt 函数：将哈希值的十六进制字符串转换为整数
func hexToInt(hexStr string) int64 {
	if len(hexStr) < 8 {
		return 0 // 如果哈希值长度不足 8，返回 0
	}
	val, err := strconv.ParseInt(hexStr[:8], 16, 64) // 取前 8 位哈希值
	if err != nil {
		return 0 // 如果转换失败，返回 0
	}
	return val
}

// fastCDC 函数：使用 FastCDC 算法将文件分块
func fastCDC(data []byte) ([][]byte, error) {
	opts := fastcdc.Options{
		MinSize:     8 * 1024,        // 最小块大小
		AverageSize: 64 * 1024,       // 平均块大小
		MaxSize:     1 * 1024 * 1024, // 最大块大小
	}

	// 创建分块器
	chunker, err := fastcdc.NewChunker(bytes.NewReader(data), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunker: %v", err)
	}

	// 分块
	var chunks [][]byte
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get next chunk: %v", err)
		}

		// 检查分块大小是否大于 MaxSize
		if chunk.Length > opts.MaxSize {
			return nil, fmt.Errorf("chunk size %d is out of range [%d, %d]", chunk.Length, opts.MinSize, opts.MaxSize)
		}

		// 独立保存分块数据
		chunkData := make([]byte, chunk.Length)
		copy(chunkData, chunk.Data)
		chunks = append(chunks, chunkData)
	}

	return chunks, nil
}

// preprocessFile 函数：对文件进行预处理，包括分块、计算哈希、组合超级块等
func preprocessFile(fileName string, fileData []byte) (FileInfo, error) {
	fileInfo := FileInfo{
		Name: fileName,
		Size: int64(len(fileData)),
		Type: filepath.Ext(fileName), // 从文件名中提取文件类型
		Hash: computeHash(fileData),
	}

	// 使用 FastCDC 分块
	chunksData, err := fastCDC(fileData)
	if err != nil {
		return FileInfo{}, fmt.Errorf("failed to chunk file: %v", err)
	}

	// 生成超级块
	var currentSuperChunk SuperChunk
	for i, chunkData := range chunksData {
		chunk := Chunk{
			Offset:      int64(i),
			Hash:        computeHash(chunkData),
			Data:        chunkData, // 存储块的二进制数据
			IsDuplicate: false,
		}

		// 将块添加到当前超级块中
		currentSuperChunk.Chunks = append(currentSuperChunk.Chunks, chunk)
		currentSuperChunk.RepresentativeHashes = append(currentSuperChunk.RepresentativeHashes, chunk.Hash)
		currentSuperChunk.FileType = filepath.Ext(fileName) // 设置文件类型
		currentSuperChunk.FileHash = fileInfo.Hash
		currentSuperChunk.StorageURL = ""
		// 检查是否满足生成超级块的条件
		if len(currentSuperChunk.Chunks) >= 30 && (len(currentSuperChunk.Chunks) == 40 || i == len(chunksData)-1 || hexToInt(chunk.Hash)%4 == 0) {
			// 创建一个长度为 8 的切片,代表指纹集只含8个指纹
			slice := make([]string, 8)
			copy(slice, currentSuperChunk.RepresentativeHashes[:8])
			currentSuperChunk.RepresentativeHashes = slice
			fileInfo.SuperChunks = append(fileInfo.SuperChunks, currentSuperChunk)
			currentSuperChunk = SuperChunk{} // 重置当前超级块
		}
	}

	// 如果最后几个块未满足条件，生成一个超级块
	if len(currentSuperChunk.Chunks) > 0 {
		if len(currentSuperChunk.RepresentativeHashes) > 8 {
			slice := make([]string, 8)
			copy(slice, currentSuperChunk.RepresentativeHashes[:8])
			currentSuperChunk.RepresentativeHashes = slice
		}
		fileInfo.SuperChunks = append(fileInfo.SuperChunks, currentSuperChunk)
	}

	return fileInfo, nil
}

// sendFileInfo 函数：发送文件信息到元数据服务器，检查文件是否重复,若不重复则进行路由获取存储位置url
func sendFileInfo(fileInfoWithData FileInfo) (bool, FileInfo) {
	url := metadataServerURL + "/processFileInfo"

	// 构造 fileInfoWithoutData，用于只发送元数据，不包含 Chunk 的实际数据
	fileInfoWithoutData := FileInfo{
		Name: fileInfoWithData.Name, // 文件名
		Size: fileInfoWithData.Size, // 文件大小
		Type: fileInfoWithData.Type, // 文件类型
		Hash: fileInfoWithData.Hash, // 文件哈希
	}

	// 遍历原始文件中的所有超级块
	for _, sc := range fileInfoWithData.SuperChunks {
		// 创建一个新的超级块结构体，拷贝元数据信息
		newSuperChunk := SuperChunk{
			FileHash:             sc.FileHash,                                       // 所属文件的哈希
			RepresentativeHashes: append([]string(nil), sc.RepresentativeHashes...), // 拷贝指纹集
			FileType:             sc.FileType,                                       // 文件类型
			StorageURL:           sc.StorageURL,                                     // 存储地址（可为空）
		}

		// 遍历该超级块中的所有 Chunk
		for _, chunk := range sc.Chunks {
			// 创建一个新的 Chunk，拷贝元数据，不保留 Data 数据
			newChunk := Chunk{
				Offset:      chunk.Offset,      // 块偏移
				Hash:        chunk.Hash,        // 块哈希
				IsDuplicate: chunk.IsDuplicate, // 是否为重复块
				Data:        []byte{},          // 清空实际数据，节省传输
			}
			newSuperChunk.Chunks = append(newSuperChunk.Chunks, newChunk)
		}

		// 将新的超级块加入目标结构体中
		fileInfoWithoutData.SuperChunks = append(fileInfoWithoutData.SuperChunks, newSuperChunk)
	}

	jsonData, err := json.Marshal(fileInfoWithoutData)
	if err != nil {
		log.Fatal("Failed to marshal file info:", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatal("Failed to send file info:", err)
	}
	defer resp.Body.Close()

	var fileInfoWithUrl FileInfo
	var isDuplicate bool = false
	//解析元数据服务器响应
	json.NewDecoder(resp.Body).Decode(&fileInfoWithUrl)
	if fileInfoWithUrl.Name == "isDuplicate" {
		isDuplicate = true
		return isDuplicate, FileInfo{}
	}
	//更新超级块的url和是否重复信息
	for i := range fileInfoWithUrl.SuperChunks {
		fileInfoWithData.SuperChunks[i].StorageURL = fileInfoWithUrl.SuperChunks[i].StorageURL
		for j := range fileInfoWithUrl.SuperChunks[i].Chunks {
			fileInfoWithData.SuperChunks[i].Chunks[j].IsDuplicate = fileInfoWithUrl.SuperChunks[i].Chunks[j].IsDuplicate
			if fileInfoWithData.SuperChunks[i].Chunks[j].IsDuplicate {
				fileInfoWithData.SuperChunks[i].Chunks[j].Data = []byte{}
			}
		}
	}
	return isDuplicate, fileInfoWithData
}

// 上传一个超级块，带重试机制
func uploadSuperChunkWithRetry(sc SuperChunk, maxRetries int) error {
	jsonData, err := json.Marshal(sc)
	if err != nil {
		return fmt.Errorf("序列化失败: %v", err)
	}

	url := fmt.Sprintf("%s/storeSuperChunks", sc.StorageURL)

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("创建请求失败: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}

		if resp != nil {
			resp.Body.Close()
		}

		log.Printf("上传失败 [第 %d 次重试] [%s]: %v", attempt, sc.FileHash, err)

		// 如果还有机会重试，休眠一下
		if attempt <= maxRetries {
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("超级块上传失败 [%s]，已尝试 %d 次", sc.FileHash, maxRetries+1)
}

// storeFileChunks 函数：将非重复块的超级块数据并发上传到对应存储节点
func storeFileChunks(fileInfo FileInfo) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failedChunks []string

	concurrency := 8
	sem := make(chan struct{}, concurrency)

	for _, superChunk := range fileInfo.SuperChunks {
		wg.Add(1)
		sem <- struct{}{} // 占用一个名额

		go func(sc SuperChunk) {
			defer wg.Done()
			defer func() { <-sem }() // 释放名额

			err := uploadSuperChunkWithRetry(sc, 2) // 最多重试 2 次
			if err != nil {
				mu.Lock()
				failedChunks = append(failedChunks, sc.FileHash)
				mu.Unlock()
			}
		}(superChunk)
	}

	wg.Wait()

	// 输出失败信息
	if len(failedChunks) > 0 {
		log.Printf("以下超级块上传失败（共 %d 个）：\n%v", len(failedChunks), failedChunks)
	}
}

// processDataset 函数：处理数据集文件夹中的所有文件
func processDataset(maxConcurrency int) (int64, int, float64, float64, float64) {
	// 遍历数据集文件夹
	processedFile := 0
	files, err := os.ReadDir(datasetDir)
	if err != nil {
		log.Fatal("Failed to read dataset directory:", err)
	}

	var (
		wg                sync.WaitGroup // 用于等待所有 Goroutines 完成
		totalFiles        int            // 总文件数
		processedFileSize int64          // 总文件大小
		totalDataSize     int64          // 各存储节点存储的数据大小
		startTime         = time.Now()   // 开始时间
	)

	// 创建一个有缓冲的通道，用于控制并发数量
	concurrencyLimit := make(chan struct{}, maxConcurrency)

	for _, file := range files {
		if !file.IsDir() {
			totalFiles++
			filePath := filepath.Join(datasetDir, file.Name())
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				log.Printf("Failed to get file info %s: %v\n", filePath, err)
				continue
			}
			processedFileSize += fileInfo.Size()

			wg.Add(1)
			concurrencyLimit <- struct{}{} // 占用一个并发槽

			go func(filePath string, fileName string) {

				defer wg.Done()
				defer func() { <-concurrencyLimit }() // 释放一个并发槽

				// 读取文件内容
				fileData, err := os.ReadFile(filePath)
				if err != nil {
					log.Printf("Failed to read file %s: %v\n", filePath, err)
					return
				}

				// 预处理文件
				fileInfo, err := preprocessFile(fileName, fileData)
				if err != nil {
					log.Printf("Failed to preprocess file %s: %v\n", filePath, err)
					return
				}

				// 发送文件信息到元数据服务器，检查文件是否重复
				isDuplicate, fileInfoWithUrl := sendFileInfo(fileInfo)
				if isDuplicate {
					//fmt.Printf("文件 %s 是重复的，跳过备份。\n", fileName)
					return
				}

				// 将超级块发送到元数据服务器进行路由
				storeFileChunks(fileInfoWithUrl)
				mu.Lock()
				processedFile++
				if processedFile%int(len(files)/4) == 0 {
					fmt.Printf("已处理%d个文件/总文件数%d\n", processedFile, len(files))
				}
				mu.Unlock()

			}(filePath, file.Name())
		}
	}
	// 等待所有 Goroutines 完成
	wg.Wait()
	fmt.Println("文件处理完成")

	// 获取并展示所有存储节点的信息
	storageInfo, err := getAllStorageInfo()
	if err != nil {
		log.Printf("获取存储节点信息失败: %v\n", err)
		return 0, 0, 0, 0, 0
	}

	fmt.Println("\n存储服务器信息：")
	fmt.Printf("存储服务器个数: %d \n", len(storageInfo.Nodes))

	maxLoad := storageInfo.Nodes[0].Load
	minLoad := storageInfo.Nodes[0].Load
	storageInfoLog := ""
	for _, node := range storageInfo.Nodes {
		fmt.Printf("节点 URL: %s, 存储大小: %d MB, 数据类型: %v\n", node.URL, node.Load/1024/1024, node.FileTypes)
		storageInfoLog += fmt.Sprintf("节点 URL: %s, 存储大小: %d MB, 数据类型: %v\n", node.URL, node.Load/1024/1024, node.FileTypes)
		if node.Load > maxLoad {
			maxLoad = node.Load
		}
		if node.Load < minLoad {
			minLoad = node.Load
		}
		totalDataSize += node.Load
	}
	fmt.Printf("数据倾斜率: %.5f\n", float64(maxLoad)/float64(minLoad))

	// 计算吞吐量和重删率
	elapsedTime := time.Since(startTime).Seconds()
	throughput := float64(processedFileSize) / elapsedTime / 1024 / 1024 // 吞吐量（MB/s）

	// 输出统计信息
	fmt.Printf("并发上传线程数: %d\n", maxConcurrency)
	fmt.Printf("总文件数: %d\n", totalFiles)
	fmt.Printf("总文件大小: %d MB\n", processedFileSize/1024/1024)
	fmt.Printf("各存储服务器总存储消耗量: %d MB\n", totalDataSize/1024/1024)
	fmt.Printf("吞吐量: %.2f MB/s\n", throughput)
	absoluteDedupRatio = float64(storageInfo.TotalFileSize) / float64(totalDataSize)
	if len(storageInfo.Nodes) == 1 { //单节点，计算绝对重删率
		oneNodeAbsoluteDedupRatio = absoluteDedupRatio
		if totalDataSize < processedFileSize {
			fmt.Printf("单节点绝对重删率: %.5f\n", oneNodeAbsoluteDedupRatio)
		}

	} else {
		haveRunSingleNode := "相对"
		if oneNodeAbsoluteDedupRatio == 1 { //如果没测过单点则无法计算多节点相对重删率，只能算绝对重删率
			haveRunSingleNode = "绝对"
		}
		relativeDedupRatio = absoluteDedupRatio / oneNodeAbsoluteDedupRatio // 多节点相对重删率=多节点绝对重删率/单节点系统的绝对重删率
		if totalDataSize < processedFileSize {
			fmt.Printf("%d节点%s重删率: %.5f\n\n", len(storageInfo.Nodes), haveRunSingleNode, relativeDedupRatio)
		}
	}

	// 将结果追加到本地文件
	file, err := os.OpenFile("result.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("无法打开文件:", err)
		return 0, 0, 0, 0, 0
	}
	defer file.Close()

	// 获取当前时间并格式化
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// 格式化输出并写入文件，包含时间戳
	result := fmt.Sprintf("[%s] 上传完成！\n存储服务器个数: %d \n%s", currentTime, len(storageInfo.Nodes), storageInfoLog)
	result += fmt.Sprintf("数据倾斜率: %.5f\n", float64(maxLoad)/float64(minLoad))
	result += fmt.Sprintf("并发上传线程数: %d\n", maxConcurrency)
	result += fmt.Sprintf("总文件数: %d\n", totalFiles)
	result += fmt.Sprintf("总文件大小: %d MB\n", processedFileSize/1024/1024)
	result += fmt.Sprintf("各存储服务器总存储消耗量: %d MB\n", totalDataSize/1024/1024)
	result += fmt.Sprintf("吞吐量: %.2f MB/s\n", throughput)
	if len(storageInfo.Nodes) == 1 { //单节点，计算绝对重删率
		if totalDataSize < processedFileSize {
			result += fmt.Sprintf("单节点绝对重删率: %.5f\n\n", oneNodeAbsoluteDedupRatio)
		}
	} else {
		haveRunSingleNode := "相对"
		if absoluteDedupRatio == 1 { //如果没测过单点则无法计算多节点相对重删率，只能结算绝对重删率
			haveRunSingleNode = "绝对"
		}
		relativeDedupRatio = absoluteDedupRatio / oneNodeAbsoluteDedupRatio // 多节点相对重删率=多节点绝对重删率/单节点系统的绝对重删率
		if totalDataSize < processedFileSize {
			result += fmt.Sprintf("%d节点%s重删率: %.5f\n\n", len(storageInfo.Nodes), haveRunSingleNode, relativeDedupRatio)
		}
	}
	if _, err := file.WriteString(result); err != nil {
		fmt.Println("写入文件失败:", err)
		return 0, 0, 0, 0, 0
	}

	fmt.Println("结果已追加到 result.log 文件中")

	return processedFileSize, len(storageInfo.Nodes), float64(maxLoad) / float64(minLoad), relativeDedupRatio, absoluteDedupRatio
}

// getAllStorageInfo 函数：调用元数据服务器的 getAllStorageInfo 接口，获取所有存储节点的信息
func getAllStorageInfo() (struct {
	TotalFileSize int64         `json:"totalFilesSize"`
	Nodes         []StorageInfo `json:"nodes"`
}, error) {
	url := metadataServerURL + "/getAllStorageInfo"
	resp, err := http.Get(url)
	if err != nil {
		return struct {
			TotalFileSize int64         `json:"totalFilesSize"`
			Nodes         []StorageInfo `json:"nodes"`
		}{}, fmt.Errorf("调用元数据服务器失败: %v", err)
	}
	defer resp.Body.Close()

	//解析响应
	var result struct {
		TotalFileSize int64         `json:"totalFilesSize"`
		Nodes         []StorageInfo `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return struct {
			TotalFileSize int64         `json:"totalFilesSize"`
			Nodes         []StorageInfo `json:"nodes"`
		}{}, fmt.Errorf("解析响应失败: %v", err)
	}

	return result, nil
}

// restoreFile 函数：根据文件名恢复文件到本地 download 文件夹
func restoreFile(fileName string, concurrency int) (int64, int, error) {

	startTime := time.Now()
	fileSize := int64(0)
	// 创建 download 文件夹
	downloadDir := "./download"
	if err := os.MkdirAll(downloadDir, os.ModePerm); err != nil {
		return 0, 0, fmt.Errorf("创建 download 文件夹失败: %v", err)
	}

	// 向元数据服务器发送文件恢复请求
	url := metadataServerURL + "/restoreFile"
	requestBody := map[string]string{
		"fileName": fileName,
	}
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return 0, 0, fmt.Errorf("序列化请求体失败: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, 0, fmt.Errorf("发送文件恢复请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("文件恢复请求失败，状态码: %s", resp.Status)
	}

	// 解析响应，获取文件的超级块信息
	var fileInfo FileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fileInfo); err != nil {
		return 0, 0, fmt.Errorf("解析响应失败: %v", err)
	}

	// 恢复文件的块
	var fileData []byte
	for _, superChunk := range fileInfo.SuperChunks {

		//fmt.Println("chunk offset:",chunk.Offset)
		// 向存储服务器请求块数据
		url := fmt.Sprintf("%s/getSuperChunk", superChunk.StorageURL)
		requestBody := superChunk
		jsonData, err := json.Marshal(requestBody)
		if err != nil {
			return 0, 0, fmt.Errorf("序列化请求体失败: %v", err)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return 0, 0, fmt.Errorf("发送块数据请求失败: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return 0, 0, fmt.Errorf("块数据请求失败，状态码: %s", resp.Status)
		}

		// 读取块数据
		superChunkData, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, 0, fmt.Errorf("读取块数据失败: %v", err)
		}

		// 将块数据写入文件
		fileData = append(fileData, superChunkData...)

	}
	fileSize = int64(len(fileData))
	// 将文件数据写入本地文件
	filePath := filepath.Join(downloadDir, fileName)
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		return 0, 0, fmt.Errorf("写入文件失败: %v", err)
	}

	fmt.Printf("文件 %s 已恢复到 %s\n", fileName, filePath)
	// 获取并展示所有存储节点的信息
	storageInfo, err := getAllStorageInfo()
	if err != nil {
		log.Printf("获取存储节点信息失败: %v\n", err)
		return 0, 0, nil
	}

	// 多线程模拟多客户端并发恢复
	var wg sync.WaitGroup
	for i := 1; i < concurrency; i++ {
		wg.Add(1)
		go func(superChunks []SuperChunk) {
			defer wg.Done()
			for _, superChunk := range superChunks {
				url := fmt.Sprintf("%s/getSuperChunk", superChunk.StorageURL)
				requestBody := superChunk
				jsonData, err := json.Marshal(requestBody)
				if err != nil {
					fmt.Printf("序列化请求体失败: %v", err)
					continue
				}

				resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					fmt.Printf("发送块数据请求失败: %v", err)
					continue
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					fmt.Printf("块数据请求失败，状态码: %s", resp.Status)
					continue
				}

			}
		}(fileInfo.SuperChunks)
	}
	wg.Wait()
	processedFileSize := fileSize * int64(concurrency)
	elapsedTime := time.Since(startTime).Seconds()
	throughput := float64(processedFileSize) / elapsedTime / 1024 / 1024 // 吞吐量（MB/s）

	fmt.Println("总大小：", processedFileSize/1024/1024, "MB")
	fmt.Println("并发量：", concurrency)
	fmt.Printf("吞吐量：%.2fMB/s\n", throughput)
	return fileSize * int64(concurrency), len(storageInfo.Nodes), nil
}

// clearMetadataServer 调用 metadata_server 的 /clear 接口
func clearMetadataServer() error {
	// 构造请求体
	requestBody := map[string]string{
		"node_url": "", // 将本机 url 作为请求体的一部分
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

// 与客户端控制器交互
var (
	clientPort         string                        // 客户端的端口号
	clientControlerURL = "http://192.168.1.108:8081" // 客户端控制器的 URL
	localURL           string                        // 本机 URL
)

func startClient(w http.ResponseWriter, r *http.Request) {
	// 解析请求体
	var requestBody struct {
		Operation  string `json:"operation"`
		Parameter  string `json:"parameter"`
		Parameter2 string `json:"parameter2"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}
	if requestBody.Operation == "upload" {
		// 处理数据集文件夹中的所有文件
		concurrentQuantity, err := strconv.Atoi(requestBody.Parameter)
		if err != nil {
			fmt.Println("输入的不是有效的数字")
			return
		}
		size, nodeNum, skewRate, relativeDedupRatio, absoluteDedupRatio := processDataset(concurrentQuantity)
		// 返回总大小和时间
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			TotalSize          int64   `json:"totalSize"`
			NodeNum            int     `json:"nodeNum"`
			SkewRate           float64 `json:"skewRate"`
			RelativeDedupRatio float64 `json:"relativeDedupRatio"`
			AbsoluteDedupRatio float64 `json:"absoluteDedupRatio"`
		}{
			TotalSize:          size,
			NodeNum:            nodeNum,
			SkewRate:           skewRate,
			RelativeDedupRatio: relativeDedupRatio,
			AbsoluteDedupRatio: absoluteDedupRatio,
		})
	}
	if requestBody.Operation == "clean" {
		// 调用 metadata_server 的 /clear 接口
		if err := clearMetadataServer(); err != nil {
			fmt.Println("Error:", err)
			return
		}
	}
	if requestBody.Operation == "restore" {
		concurrentQuantity, err := strconv.Atoi(requestBody.Parameter2)
		if err != nil {
			fmt.Println("输入的不是有效的数字")
			return
		}
		// 调用恢复文件函数
		size, nodeNum, err := restoreFile(requestBody.Parameter, concurrentQuantity)
		if err != nil {
			fmt.Println("恢复文件失败:", err)
		}
		// 返回总大小和数据类型
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(struct {
			TotalSize int64 `json:"totalSize"`
			NodeNum   int   `json:"nodeNum"`
		}{
			TotalSize: size,
			NodeNum:   nodeNum,
		})
	}
}

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

// 向客户端控制器注册自己
func registerWithClientControler() error {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("请输客户端控制器 IP 地址，输入为空则按默认ip(%s)连接：", clientControlerURL)
	ipInputIp, _ := reader.ReadString('\n')
	ipInputIp = strings.TrimSpace(ipInputIp) // 去除输入中的换行符和空格
	if ipInputIp != "" {
		clientControlerURL = "http://" + ipInputIp + ":8081"
	}

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
	clientPort = ":" + strconv.Itoa(port)

	// 构造注册请求
	localURL = "http://" + ip + clientPort
	clientInfo := map[string]interface{}{
		"url": localURL, // 数据节点的 URL
	}
	jsonData, err := json.Marshal(clientInfo)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %v", err)
	}

	// 发送注册请求
	req, err := http.NewRequest("POST", clientControlerURL+"/register", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("创建注册请求失败: %v", err)
	}
	req.Header.Set("client-URL", "http://"+ip+clientPort)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送注册请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("注册请求失败，状态码: %s", resp.Status)
	}

	fmt.Println("成功注册到客户端控制器")
	return nil
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("请输入元数据服务器 IP 地址，输入为空则按默认ip(%s)连接：", metadataServerURL)
	ipInputIp, _ := reader.ReadString('\n')
	ipInputIp = strings.TrimSpace(ipInputIp) // 去除输入中的换行符和空格
	if ipInputIp != "" {
		metadataServerURL = "http://" + ipInputIp + ":8080"
	}

	http.HandleFunc("/startClient", startClient)

	input := ""
	registed := false
	for input != "exit" {
		fmt.Println("-------------------------------------------------------")
		fmt.Println("输入\"upload\"上传文件，\"clean\"清除所有数据，\"restore\"恢复文件，\"auto\"交由客户端并发控制器控制，\"exit\"退出")
		input, _ = reader.ReadString('\n')
		input = strings.TrimSpace(input) // 去除输入中的换行符和空格
		if input == "upload" {
			// 处理数据集文件夹中的所有文件
			fmt.Printf("请输入并发上传数量：")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input) // 去除输入中的换行符和空格
			concurrentQuantity, err := strconv.Atoi(input)
			if err != nil {
				fmt.Println("输入的不是有效的数字")
				return
			}
			processDataset(concurrentQuantity)
		}
		if input == "clean" {
			// 调用 metadata_server 的 /clear 接口
			if err := clearMetadataServer(); err != nil {
				fmt.Println("Error:", err)
				return
			}
		}
		if input == "restore" {
			// 输入要恢复的文件名
			fmt.Print("请输入要恢复的文件名: ")
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)
			fmt.Printf("请输入并发数量：")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input) // 去除输入中的换行符和空格
			concurrentQuantity, err := strconv.Atoi(input)
			if err != nil {
				fmt.Println("输入的不是有效的数字")
				return
			}
			// 调用恢复文件函数
			restoreFile(fileName, concurrentQuantity)

		}
		if input == "auto" && !registed {
			// 注册存储节点
			if err := registerWithClientControler(); err != nil {
				fmt.Println("注册到客户端控制器失败:", err)
				os.Exit(1)
			}
			// 启动客户端控制器（非阻塞）
			go func() {
				fmt.Printf("客户端运行在端口 %s...\n", clientPort)
				log.Fatal(http.ListenAndServe(clientPort, nil))
			}()
			registed = true
		}
	}
}
