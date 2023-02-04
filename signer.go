package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	md5Mutex sync.Mutex
)

// ExecutePipeline обеспечивает нам конвейерную обработку функций-воркеров, которые что-то делают
func ExecutePipeline(hashSignJobs ...job) {
	wg := sync.WaitGroup{}
	in := make(chan any)

	for _, job := range hashSignJobs {
		wg.Add(1)
		out := make(chan any)
		go workerPipeline(&wg, job, in, out)
		in = out
	}

	wg.Wait()
}

func workerPipeline(wg *sync.WaitGroup, jobFunc job, in, out chan any) {
	defer wg.Done()
	defer close(out)
	jobFunc(in, out)
}

// SingleHash считает значение crc32(data)+"~"+crc32(md5(data)) ( конкатенация двух строк через ~), где data - то что пришло на вход (по сути - числа из первой функции)
func SingleHash(in chan any, out chan any) {
	wg := sync.WaitGroup{}

	for data := range in {
		str := fmt.Sprintf("%v", data)
		strMd5 := GetMD5WithoutCoolDown(str)
		wg.Add(1)
		go workerSingleHash(&wg, str, strMd5, out)
	}
	wg.Wait()
}

func workerSingleHash(wg *sync.WaitGroup, data, md string, ch chan any) {
	defer wg.Done()

	crc32Chan := make(chan string)
	crcMd5Chan := make(chan string)

	go calculateHash(crc32Chan, data, DataSignerCrc32)
	go calculateHash(crcMd5Chan, md, DataSignerCrc32)

	crc32Hash := <-crc32Chan
	crc32Md5Hash := <-crcMd5Chan

	ch <- crc32Hash + "~" + crc32Md5Hash

}

func calculateHash(crc32Chan chan string, data string, hashCalculator func(data string) string) {
	result := hashCalculator(data)
	crc32Chan <- result
}

// MultiHash считает значение crc32(th+data)) (конкатенация цифры, приведённой к строке и строки), где th=0..5 ( т.е. 6 хешей на каждое входящее значение ),
// потом берёт конкатенацию результатов в порядке расчета (0..5), где data - то что пришло на вход (и ушло на выход из SingleHash)
func MultiHash(in chan any, out chan any) {
	wg := sync.WaitGroup{}

	for data := range in {
		wg.Add(1)
		go workerMultiHash(&wg, data, out)
	}

	wg.Wait()
}

func workerMultiHash(wg *sync.WaitGroup, data any, out chan any) {
	wgInternal := sync.WaitGroup{}
	results := make([]string, 6)
	defer wg.Done()

	for i := 0; i < 6; i++ {
		wgInternal.Add(1)
		d := fmt.Sprintf("%v%v", i, data)
		go calculateMultiHash(&wgInternal, d, results, i)
	}

	wgInternal.Wait()

	out <- strings.Join(results, "")
}

func calculateMultiHash(wgInternal *sync.WaitGroup, data string, res []string, i int) {
	defer wgInternal.Done()
	res[i] = DataSignerCrc32(data)
}

// CombineResults получает все результаты, сортирует (https://golang.org/pkg/sort/), объединяет отсортированный результат через _ (символ подчеркивания) в одну строку
func CombineResults(in, out chan any) {
	input := make([]string, 0)

	for data := range in {
		input = append(input, data.(string))
	}

	sort.Strings(input)
	out <- strings.Join(input, "_")
}

func GetMD5WithoutCoolDown(data string) string {
	md5Mutex.Lock()
	defer md5Mutex.Unlock()
	return DataSignerMd5(data)
}
