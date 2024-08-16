/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_sundera96 {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {
        var clockStart = System.currentTimeMillis();
        calculate();
        System.out.format("Took %,d ms\n", System.currentTimeMillis() - clockStart);
    }

    private static void calculate() throws Exception {
        final File file = new File(FILE);
        final long fileLength = file.length();
        final int numOfChunks = Runtime.getRuntime().availableProcessors();
        final StationStat[][] results = new StationStat[numOfChunks][];
        final long[] cursorPos = new long[numOfChunks];
        try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < numOfChunks; i++) {
                long cursorStart = fileLength * i / numOfChunks;
                raf.seek(cursorStart);
                while (raf.read() != (byte) '\n')
                    ;
                cursorPos[i] = raf.getFilePointer();
            }
            final MemorySegment memorySegment = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileLength, Arena.global());
            final Thread[] threads = new Thread[numOfChunks];
            for (int i = 0; i < numOfChunks; i++) {
                long start = cursorPos[i];
                long end = i + 1 < numOfChunks ? cursorPos[i + 1] : fileLength;
                threads[i] = new Thread(new ChunkProcessor(memorySegment.asSlice(start, end - start), results, i));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
            var totalsMap = new TreeMap<String, StationStat>();
            for (StationStat[] statsArray : results) {
                for (StationStat stats : statsArray) {
                    totalsMap.merge(stats.name, stats, (old, curr) -> {
                        old.count += curr.count;
                        old.sum += curr.sum;
                        old.min = Math.min(old.min, curr.min);
                        old.max = Math.max(old.max, curr.max);
                        return old;
                    });
                }
            }
            System.out.println(totalsMap);
        }
    }

    public static class ChunkProcessor implements Runnable {
        private final MemorySegment memorySegment;
        private final StationStat[][] results;
        private final int threadId;
        private final Map<String, StationStat> hashMap;

        public ChunkProcessor(final MemorySegment memorySegment, final StationStat[][] results, final int threadId) {
            this.memorySegment = memorySegment;
            this.results = results;
            this.threadId = threadId;
            hashMap = new HashMap<>();
        }

        @Override
        public void run() {
            for (long cursor = 0; cursor < memorySegment.byteSize();) {
                long semicolonPos = findByte(cursor, (byte) ';');
                long newLinePos = findByte(semicolonPos + 1, (byte) '\n');
                String name = getStringVal(cursor, semicolonPos);
                int temp = getTemperatureVal(semicolonPos);
                hashMap.computeIfAbsent(name, k -> new StationStat(name));
                StationStat stat = hashMap.get(name);
                stat.sum += temp;
                stat.count += 1;
                stat.min = Math.min(stat.min, temp);
                stat.max = Math.max(stat.max, temp);
                cursor = newLinePos + 1;
            }
            results[threadId] = hashMap.values().toArray(StationStat[]::new);
        }

        private long findByte(long cursor, byte delimeter) {
            for (long i = cursor; i < memorySegment.byteSize(); i++) {
                if (memorySegment.get(ValueLayout.JAVA_BYTE, i) == delimeter) {
                    return i;
                }
            }
            throw new RuntimeException((char) delimeter + " not fuund");
        }

        private String getStringVal(long start, long limit) {
            return new String(memorySegment.asSlice(start, limit - start).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }

        private int getTemperatureVal(long start) {
            long offset = start + 1;
            int sign = 1;
            byte signByte = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);
            if (signByte == '-') {
                sign = -1;
                signByte = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);
            }
            int temp = signByte - '0';
            signByte = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);
            if (signByte != '.') {
                temp = 10 * temp + (signByte - '0');
                offset++;
            }
            signByte = memorySegment.get(ValueLayout.JAVA_BYTE, offset);
            temp = 10 * temp + signByte - '0';
            return sign * temp;
        }
    }

    private static class StationStat implements Comparable<StationStat> {
        public String name;
        public long sum;
        public int count;
        public int min;
        public int max;

        public StationStat(String name) {
            this.name = name;
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
        }

        @Override
        public int compareTo(StationStat stationStat) {
            return name.compareTo(stationStat.name);
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }
    }
}