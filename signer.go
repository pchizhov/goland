package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for data := range in {
		value, ok := data.(string)
		if !ok {
			value = strconv.Itoa(data.(int))
		}

		wg.Add(1)
		go func(data string) {
			ch1 := make(chan string)
			ch2 := make(chan string)

			go func() {
				ch1 <- DataSignerCrc32(data)
			}()

			go func() {
				mu.Lock()
				md5hash := DataSignerMd5(data)
				mu.Unlock()
				ch2 <- DataSignerCrc32(md5hash)
			}()

			out <- <- ch1 + "~" + <- ch2
			wg.Done()
		}(value)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		value, ok := data.(string)
		if !ok {
			value = strconv.Itoa(data.(int))
		}

		wg.Add(1)
		go func(step1 string) {
			workers := &sync.WaitGroup{}
			dataHashes := make([]string, 6)
			for th := 0; th < 6; th++ {
				workers.Add(1)
				go func(th int) {
					dataHashes[th] = DataSignerCrc32(strconv.Itoa(th) + step1)
					workers.Done()
				}(th)
			}
			workers.Wait()
			out <- strings.Join(dataHashes, "")
			wg.Done()
		}(value)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := make([]string, 0)
	for data := range in {
		results = append(results, data.(string))
	}
	sort.Strings(results)
	result := strings.Join(results, "_")
	out <- result
}

func ExecutePipeline(tasks ...job) {
	wg := &sync.WaitGroup{}

	in := make(chan interface{}, MaxInputDataLen)
	out := make(chan interface{}, MaxInputDataLen)
	for _, task := range tasks {
		wg.Add(1)
		go func(wg *sync.WaitGroup, task job, in, out chan interface{}) {
			task(in, out)
			close(out)
			wg.Done()
		}(wg, task, in, out)
		in = out
		out = make(chan interface{}, MaxInputDataLen)
	}
	wg.Wait()
	close(out)
}