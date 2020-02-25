package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/pipeline/pipeline"
)

func main() {
	p := createPipeline("small.in", 512, 4)
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
	for v := range p {
		fmt.Println(v)
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
