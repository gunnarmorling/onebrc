# 1brc in go

The following exercise was committed past the deadline, so it will not be included on the leaderboard.

 Implementation description by steps:

 Reads in Chunks:

 - The readChunk function reads a specified amount of data (defined by chunkSizeMB).
 - Each call to readChunk returns the next portion of the file, so you can process a single chunk without loading the entire file into memory.

 Iterate Until EOF:

 - In the main loop, readChunk is called repeatedly until it reads the entire file.
 - When readChunk reaches the end of the file, it will return an empty slice of lines, which ends the loop.

 Process Each Chunk:

 - For each chunk, the lines are split further into smaller segments that worker goroutines can process in parallel.

 Finalize and Merge Results:

 - After all chunks are processed, the results from each worker are merged to produce a final summary.


 Key design decision

 - Decouple algorithm states by channels (Read the file in chunks, calculate stats per chunk/thread, and combine threads stats into final summary stat.)
 - Access to the filesystem is the main bottleneck, so it is better to access it as little as possible. It is better to load the file into memory (or a chunk) rather than keep hitting the file multiple times.

 Limitations

 Based on what I see in golang profiler charts (go tool pprof -http=":8081" profiles/1730046226/measurements.txt.cpu.pprof) I found the foolowing improvements:
 - strings.Split is extremely expensive, consuming about 18.8% of resources
 - The strconv.ParseFloat function is quite expensive in terms of CPU usage, consuming around 7.7% of resources

## Compile & Run

Compile: `./prepare_pjgg.sh`

Run: `./calculate_average_pjgg.sh`