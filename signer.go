package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type multiHashResult struct {
	number int
	hash   string
}

func ExecutePipeline(funcs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	for _, f := range funcs {
		wg.Add(1)
		out := make(chan interface{})
		go func(f job, in chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			f(in, out)
		}(f, in, out, wg)
		in = out
	}
	defer wg.Wait()
}

var cnt = 6

func MultiHash(in chan interface{}, out chan interface{}) {
	wg := &sync.WaitGroup{}
	outTemp := make(chan string)
	for input := range in {
		wg.Add(1)
		wgTemp := &sync.WaitGroup{}
		data := input.(string)
		inCh := make(chan multiHashResult)

		wgTemp.Add(cnt)

		for i := 0; i < cnt; i++ {
			go getMultiHash(inCh, wgTemp, data, i)
		}

		go func(wgInner *sync.WaitGroup, c chan multiHashResult) {
			defer close(c)
			wgInner.Wait()
		}(wgTemp, inCh)

		go sortMultiResults(inCh, outTemp, wg)
	}

	go func(wgOut *sync.WaitGroup, c chan string) {
		defer close(c)
		wgOut.Wait()
	}(wg, outTemp)

	for hash := range outTemp {
		out <- hash
	}

}

func SingleHash(input chan interface{}, output chan interface{}) {
	wg := &sync.WaitGroup{}
	myHash := make(chan string)
	for data := range input {
		wg.Add(1)

		data := fmt.Sprintf("%v", data)
		hashMd5 := DataSignerMd5(data)

		go func(data string, hashMd5 string) {
			defer wg.Done()

			if data == "8" {
				defer close(myHash)
			}

			crt32DataChan := getCrt32Data(data)
			right32 := DataSignerCrc32(hashMd5)
			left32 := <-crt32DataChan
			myHash <- left32 + "~" + right32
		}(data, hashMd5)
	}

	for hash := range myHash {
		output <- hash
	}

	defer wg.Wait()
}

func getMultiHash(hashResultChan chan multiHashResult, wg *sync.WaitGroup, singleHash interface{}, i int) {
	defer wg.Done()
	hashResultChan <- multiHashResult{number: i, hash: DataSignerCrc32(fmt.Sprintf("%v%v", i, singleHash))}
}

func sortMultiResults(hashResults chan multiHashResult, out chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	result := map[int]string{}
	var data []int
	for hashResult := range hashResults {
		result[hashResult.number] = hashResult.hash
		data = append(data, hashResult.number)
	}
	sort.Ints(data)
	var results []string
	for i := range data {
		results = append(results, result[i])
	}
	out <- strings.Join(results, "")
}

func getCrt32Data(data string) chan string {
	result := make(chan string, 1)
	go func(out chan<- string) {
		out <- DataSignerCrc32(data)
	}(result)
	return result
}

func CombineResults(in, out chan interface{}) {
	var hashResults []string
	var result string
	for hashResult := range in {
		hashResults = append(hashResults, (hashResult).(string))
	}
	sort.Strings(hashResults)
	result = strings.Join(hashResults, "_")
	out <- result
}
