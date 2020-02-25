package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/pipeline/pipeline"
)

func main() {
	p := createNetworkPipeline(
		"small.in", 512, 4)
	writeToFile(p, "small.out")
	printFile("small.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriterSink(writer, p)
}

func createPipeline(filename string,
	filesize, chunkCount int) <-chan int {
	// TODO 这里可能存在chunkSize不能除尽的问题（应该使用进一取整）
	chunkSize := filesize / chunkCount
	pipeline.Init()

	sourceResult := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		// TODO 这里需要把file放到数组中也返回出去，然后释放掉连接句柄
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		sourceResult = append(sourceResult, pipeline.InMemSort(source))
	}
	return pipeline.MergeN(sourceResult...)
}

func createNetworkPipeline(filename string,
	filesize, chunkCount int) <-chan int {
	// TODO 这里可能存在chunkSize不能除尽的问题（应该使用进一取整）
	chunkSize := filesize / chunkCount
	pipeline.Init()

	sortAddr := []string{}
	for i := 0; i < chunkCount; i++ {
		// TODO 这里需要把file放到数组中也返回出去，然后释放掉连接句柄
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		// 通过网络传到其它机器去
		addr := ":" + strconv.Itoa(7000+i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	sourceResults := []<-chan int{}
	for _, addr := range sortAddr {
		sourceResults = append(sourceResults,
			pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sourceResults...)
}
