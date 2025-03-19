package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/xuri/excelize/v2"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ClientControler 结构体：元数据服务器，负责文件索引和路由
type ClientControler struct {
	Clients []Client   // 存储服务器列表
	mu      sync.Mutex // 互斥锁
}

type Client struct {
	URL string `json:"url"` // 存储服务器的 URL
}

// NewClientControler 函数：初始化元数据服务器
func NewClientControler() *ClientControler {
	return &ClientControler{
		Clients: []Client{},
	}
}

// handleRegister 函数：处理存储节点注册请求
func (m *ClientControler) handleRegister(w http.ResponseWriter, r *http.Request) {
	fmt.Println("收到存储节点注册请求")

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取请求体失败", http.StatusBadRequest)
		return
	}

	// 解析请求体
	var nodeInfo Client
	if err := json.Unmarshal(body, &nodeInfo); err != nil {
		http.Error(w, "解析请求体失败", http.StatusBadRequest)
		return
	}

	// 检查 URL 是否为空
	if nodeInfo.URL == "" {
		http.Error(w, "节点 URL 缺失", http.StatusBadRequest)
		return
	}

	// 将客户端信息存储到 Clients 中
	m.mu.Lock()
	m.Clients = append(m.Clients, nodeInfo)
	m.mu.Unlock()

	fmt.Printf("客户端注册成功: %+v\n", nodeInfo)
	fmt.Println("当前客户端信息：")
	for _, node := range m.Clients {
		fmt.Println(node.URL)
	}
	fmt.Println("----------------------------")
	w.WriteHeader(http.StatusOK)
}

func startClient(clientControler *ClientControler, operation string, parameter string, parameter2 string) {
	totalSize := int64(0)
	nodeNum := 0
	clientNum := len(clientControler.Clients)
	skewRate := float64(0)
	relativeDedupRatio := float64(0)
	absoluteDedupRatio := float64(0)
	startTime := time.Now()
	// 并发调用所有客户端的startClient接口
	var wg sync.WaitGroup
	for i, client := range clientControler.Clients {
		wg.Add(1)
		go func(i int, clientURL string, operation string, parameter string, parameter2 string) {
			fmt.Println("客户端",clientURL,"开始处理文件上传")
			defer wg.Done()
			url := fmt.Sprintf("%s/startClient", clientURL)
			// 构造请求体
			requestBody := struct {
				Operation  string `json:"operation"`
				Parameter  string `json:"parameter"`
				Parameter2 string `json:"parameter2"`
			}{
				Operation:  operation,
				Parameter:  parameter,
				Parameter2: parameter2,
			}
			if i+1 < len(clientControler.Clients) && operation == "upload" {
				requestBody.Parameter = "4" //默认上传线程数为4
			}
			if i+1 < len(clientControler.Clients) && operation == "upload" {
				requestBody.Parameter2 = "4" //默认恢复线程数为4
			}
			jsonData, _ := json.Marshal(requestBody)
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("调用客户端 %s 的startClient接口失败: %v\n", clientURL, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("存储服务器 %s 的 clean 接口返回错误: %s\n", clientURL, resp.Status)
				return
			}
			if operation == "upload" {
				// 解析响应
				var result struct {
					TotalSize          int64   `json:"totalSize"`
					NodeNum            int     `json:"nodeNum"`
					SkewRate           float64 `json:"skewRate"`
					RelativeDedupRatio float64 `json:"relativeDedupRatio"`
					AbsoluteDedupRatio float64 `json:"absoluteDedupRatio"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					log.Fatal("Failed to decode response:", err)
				}
				fmt.Println("客户端", clientURL, "处理结果：", result)
				totalSize += result.TotalSize
				nodeNum = result.NodeNum
				skewRate = result.SkewRate
				relativeDedupRatio = result.RelativeDedupRatio
				absoluteDedupRatio = result.AbsoluteDedupRatio
			}
			if operation == "restore" {
				// 解析响应
				var result struct {
					TotalSize int64 `json:"totalSize"`
					NodeNum   int   `json:"nodeNum"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					log.Fatal("Failed to decode response:", err)
				}
				fmt.Println("客户端", clientURL, "处理结果：", result)
				totalSize += result.TotalSize
				nodeNum = result.NodeNum
			}

		}(i, client.URL, operation, parameter, parameter2)
	}
	wg.Wait()
	totalTime := time.Since(startTime).Seconds()
	throughput := float64(totalSize) / totalTime / 1024 / 1024 // 吞吐量（MB/s）
	throughputFmt := fmt.Sprintf("%.2f", throughput)
	fmt.Println("处理总文件大小：", totalSize/1024/1024, " MB")
	fmt.Println("存储服务器数量：", nodeNum)
	if operation == "upload" {
		concurrentQuantity, _ := strconv.Atoi(parameter)
		fmt.Println("上传线程数: ", 4*(clientNum-1)+concurrentQuantity)
		fmt.Println("Write performance: ", throughputFmt, " MB/s")
		fmt.Println("倾斜率: ", skewRate)
		fmt.Println("相对重删率: ", relativeDedupRatio)
		fmt.Println("绝对重删率: ", absoluteDedupRatio)
		writeResultToExcel(
			"upload",
			totalSize,
			nodeNum,                          // 存储服务器数量
			4*(clientNum-1)+concurrentQuantity, // 上传并发参数
			throughputFmt,
			skewRate,           // 倾斜率
			relativeDedupRatio, // 相对重删率
			absoluteDedupRatio,
		)
	}
	if operation == "restore" {
		concurrentQuantity, _ := strconv.Atoi(parameter2)
		fmt.Println("恢复线程数: ", 4*(clientNum-1)+concurrentQuantity)
		fmt.Println("Read performance: ", throughputFmt, " MB/s")
		writeResultToExcel(
			"restore",
			totalSize,
			nodeNum,                          // 存储服务器数量
			4*(nodeNum-1)+concurrentQuantity, // 上传并发参数
			throughputFmt,
			0,
			0,
			0,
		)
	}
}

func writeResultToExcel(operation string, totalSize int64, nodeNum int, concurrentQuantity int, throughput string, skewRate float64, relativeDedupRatio float64,absoluteDedupRatio float64) {

	currentTime := time.Now().Format("2006-01-02 15:04:05") // 当前时间

	// 打开或创建 Excel 文件
	filePath := "result.xlsx"
	var f *excelize.File
	var err error

	if _, err = os.Stat(filePath); os.IsNotExist(err) {
		f = excelize.NewFile()
	} else {
		f, err = excelize.OpenFile(filePath)
		if err != nil {
			fmt.Println("打开 Excel 文件失败:", err)
			return
		}
	}

	if operation == "upload" {
		sheet := "upload"
		if index, err := f.GetSheetIndex(sheet); err != nil || index == -1 {
			f.NewSheet(sheet)
			f.SetSheetRow(sheet, "A1", &[]interface{}{
				"处理总文件大小(MB)", "存储服务器数量", "上传线程数",
				"Write performance(MB/s)", "倾斜率", "相对重删率","相绝对重删率", "记录时间",
			})
		}

		rows, _ := f.GetRows(sheet)
		nextRow := len(rows) + 1
		cell := fmt.Sprintf("A%d", nextRow)

		f.SetSheetRow(sheet, cell, &[]interface{}{
			totalSize / 1024 / 1024,
			nodeNum,
			concurrentQuantity,
			throughput,
			skewRate,
			relativeDedupRatio,
			absoluteDedupRatio,
			currentTime,
		})
	}

	if operation == "restore" {
		sheet := "restore"
		if index, err := f.GetSheetIndex(sheet); err != nil || index == -1 {
			f.NewSheet(sheet)
			f.SetSheetRow(sheet, "A1", &[]interface{}{
				"处理总文件大小(MB)", "存储服务器数量", "恢复线程数",
				"Read performance(MB/s)", "记录时间",
			})
		}

		rows, _ := f.GetRows(sheet)
		nextRow := len(rows) + 1
		cell := fmt.Sprintf("A%d", nextRow)

		f.SetSheetRow(sheet, cell, &[]interface{}{
			totalSize / 1024 / 1024,
			nodeNum,
			concurrentQuantity,
			throughput,
			currentTime,
		})
	}

	// 保存 Excel 文件
	if err := f.SaveAs(filePath); err != nil {
		fmt.Println("保存 Excel 文件失败:", err)
	}
}

func main() {
	clientControler := NewClientControler()
	http.HandleFunc("/register", clientControler.handleRegister)

	// 启动客户端控制器（非阻塞）
	go func() {
		fmt.Println("客户端控制器运行在端口 8081...")
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	reader := bufio.NewReader(os.Stdin)
	input := ""
	for input != "exit" {
		fmt.Println("-------------------------------------------------------")
		fmt.Println("输入\"upload\"上传文件，\"clean\"清除所有数据，\"restore\"恢复文件，\"exit\"退出")
		input, _ = reader.ReadString('\n')
		input = strings.TrimSpace(input) // 去除输入中的换行符和空格
		if input == "upload" {
			fmt.Printf("请输入最后一个客户端并发上传数量：")
			inputNum, _ := reader.ReadString('\n')
			inputNum = strings.TrimSpace(inputNum) // 去除输入中的换行符和空格
			// 检查输入是否为数字
			if _, err := strconv.Atoi(inputNum); err != nil {
				fmt.Println("输入无效，请输入一个数字")
				continue
			}
			startClient(clientControler, "upload", inputNum, "")
		}
		if input == "clean" {
			startClient(clientControler, "clean", "", "")
		}
		if input == "restore" {
			// 输入要恢复的文件名
			fmt.Print("请输入要恢复的文件名: ")
			fileName, _ := reader.ReadString('\n')
			fileName = strings.TrimSpace(fileName)
			fmt.Printf("请输入客户端并发数量：")
			concurrentQuantity, _ := reader.ReadString('\n')
			concurrentQuantity = strings.TrimSpace(concurrentQuantity) // 去除输入中的换行符和空格
			//concurrentQuantity, err := strconv.Atoi(concurrentQuantity)

			startClient(clientControler, "restore", fileName, concurrentQuantity)
		}
	}
}
