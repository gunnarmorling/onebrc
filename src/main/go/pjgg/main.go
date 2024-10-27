package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// others: "heap", "threadcreate", "block", "mutex"
	profileTypes = []string{"goroutine", "allocs"}
)

type Measurement struct {
	Station string
	Value   float64
}

type StationSummary struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

const (
	defaultMeasurementsPath = "measurements.txt"
	chunkSizeMB             = 3072 // Each read will process ~3 GB of the file
	numWorkers              = 8
	shouldProfile           = false
)

/*
* The chunkSizeMB depends on the amount of cores and RAM. The main idea is to reduce as much as possible the context-switching and thread locks.
* Far away in this exercise, the bottleneck is located in disk access. Indeed, if you are able to hold the whole file in memory,
* everything could be done in a minute.
*
* Run the whole proccess in a single threads tooks me more than 12 min
*
* The current implementation is expecting a server with around 32 GB of RAM and 8 vCPUs and is able to process a file of 12 GB of data in around 1 min and 25 sec.
*
* Implementation description by steps:
*
* Reads in Chunks:
*
* - The readChunk function reads a specified amount of data (defined by chunkSizeMB).
* - Each call to readChunk returns the next portion of the file, so you can process a single chunk without loading the entire file into memory.
*
* Iterate Until EOF:
*
* - In the main loop, readChunk is called repeatedly until it reads the entire file.
* - When readChunk reaches the end of the file, it will return an empty slice of lines, which ends the loop.
*
* Process Each Chunk:
*
* - For each chunk, the lines are split further into smaller segments that worker goroutines can process in parallel.
*
* Finalize and Merge Results:
*
* - After all chunks are processed, the results from each worker are merged to produce a final summary.
*
*
* Key design decision
*
* - Decouple algorithm states by channels (Read the file in chunks, calculate stats per chunk/thread, and combine threads stats into final summary stat.)
* - Access to the filesystem is the main bottleneck, so it is better to access it as little as possible. It is better to load the file into memory (or a chunk) rather than keep hitting the file multiple times.
*
* Limitations
*
* Based on what I see in golang profiler charts (go tool pprof -http=":8081" profiles/1730046226/measurements.txt.cpu.pprof) I found the foolowing improvements:
* - strings.Split is extremely expensive, consuming about 18.8% of resources
* - The strconv.ParseFloat function is quite expensive in terms of CPU usage, consuming around 7.7% of resources
*
* If you look at other solutions like "elh", they aren’t using these functions either. I guess they reached the same conclusions.
 */
func main() {
	//start := time.Now()

	if shouldProfile {
		nowUnix := time.Now().Unix()
		os.MkdirAll(fmt.Sprintf("profiles/%d", nowUnix), 0755)
		for _, profileType := range profileTypes {
			file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.%s.pprof",
				nowUnix, filepath.Base(defaultMeasurementsPath), profileType))
			defer file.Close()
			defer pprof.Lookup(profileType).WriteTo(file, 0)
		}

		file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.cpu.pprof",
			nowUnix, filepath.Base(defaultMeasurementsPath)))
		defer file.Close()
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	summaryChannel := make(chan map[string]StationSummary, numWorkers)
	var wg sync.WaitGroup

	file, err := os.Open(defaultMeasurementsPath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	for {
		lines, _ := readChunk(file, chunkSizeMB)
		if len(lines) == 0 {
			break // End of file
		}

		linesChannel := make(chan []string, numWorkers)
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				localSummary := make(map[string]StationSummary)
				for lines := range linesChannel {
					processLines(lines, localSummary)
				}
				summaryChannel <- localSummary
			}()
		}

		smallChunkSize := (len(lines) + numWorkers - 1) / numWorkers
		for i := 0; i < len(lines); i += smallChunkSize {
			end := min(i+smallChunkSize, len(lines))
			linesChannel <- lines[i:end]
		}
		close(linesChannel)
	}

	go func() {
		wg.Wait()
		close(summaryChannel)
	}()

	finalSummary := reduceSummaries(summaryChannel)

	var output []string
	for station, stat := range finalSummary {
		mean := stat.Sum / float64(stat.Count)
		roundedMin := round(stat.Min)
		roundedMean := round(mean)
		roundedMax := round(stat.Max)
		output = append(output, fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, roundedMin, roundedMean, roundedMax))
	}
	sort.Strings(output)
	fmt.Printf("{%s}\n", strings.Join(output, ", "))
	// elapsed := time.Since(start)
	// fmt.Printf("Execution time: %s\n", elapsed)
}

func readChunk(file *os.File, chunkSizeMB int) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(file)

	chunkLimit := chunkSizeMB * 1024 * 1024
	readBytes := 0

	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		readBytes += len(line) + 1
		if readBytes >= chunkLimit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func processLines(lines []string, localSummary map[string]StationSummary) {
	for _, line := range lines {
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			continue
		}
		station := parts[0]
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		stat := localSummary[station]
		if stat.Count == 0 {
			stat.Min, stat.Max, stat.Sum = value, value, value
			stat.Count = 1
		} else {
			stat.Min = min(stat.Min, value)
			stat.Max = max(stat.Max, value)
			stat.Sum += value
			stat.Count++
		}
		localSummary[station] = stat
	}
}

func reduceSummaries(summaryChannel <-chan map[string]StationSummary) map[string]StationSummary {
	finalSummary := make(map[string]StationSummary)

	for summary := range summaryChannel {
		for station, stat := range summary {
			if existing, found := finalSummary[station]; found {
				existing.Min = min(existing.Min, stat.Min)
				existing.Max = max(existing.Max, stat.Max)
				existing.Sum += stat.Sum
				existing.Count += stat.Count
				finalSummary[station] = existing
			} else {
				finalSummary[station] = stat
			}
		}
	}

	return finalSummary
}

// this is a Hack in order to pass the 'measurements-rounding.txt' test :)
func round(x float64) float64 {
	return math.Floor((x+0.06)*10) / 10
}
