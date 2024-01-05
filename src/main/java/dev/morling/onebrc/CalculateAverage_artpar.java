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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_artpar {
    public static final int N_THREADS = 8;
    private static final String FILE = "./measurements.txt";
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    // final int VECTOR_SIZE = 512;
    // final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
    final int SIZE = 1024 * 128;

    public CalculateAverage_artpar() throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        long fileSize = Files.size(measurementFile);

        long expectedChunkSize = fileSize / N_THREADS;

        ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);

        long chunkStartPosition = 0;
        RandomAccessFile fis = new RandomAccessFile(measurementFile.toFile(), "r");
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        long bytesReadCurrent = 0;

        try (FileChannel fileChannel = FileChannel.open(measurementFile, StandardOpenOption.READ)) {
            for (int i = 0; i < N_THREADS; i++) {

                long chunkSize = expectedChunkSize;
                chunkSize = fis.skipBytes(Math.toIntExact(chunkSize));

                bytesReadCurrent += chunkSize;
                while (((char) fis.read()) != '\n' && bytesReadCurrent < fileSize) {
                    chunkSize++;
                    bytesReadCurrent++;
                }

                // System.out.println("[" + chunkStartPosition + "] - [" + (chunkStartPosition + chunkSize) + " bytes");
                if (chunkStartPosition + chunkSize >= fileSize) {
                    chunkSize = fileSize - chunkStartPosition;
                }
                if (chunkSize > Integer.MAX_VALUE) {
                    chunkSize = Integer.MAX_VALUE;
                }

                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
                        chunkSize);

                ReaderRunnable readerRunnable = new ReaderRunnable(mappedByteBuffer);
                Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
                // System.out.println("Added future [" + chunkStartPosition + "][" + chunkSize + "]");
                futures.add(future);
                chunkStartPosition = chunkStartPosition + chunkSize + 1;
            }
        }
        fis.close();

        Map<String, MeasurementAggregator> globalMap = futures.parallelStream()
                .flatMap(future -> {
                    try {
                        return future.get().entrySet().stream();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }).parallel().collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue,
                        MeasurementAggregator::combine));

        Map<String, ResultRow> results = globalMap.entrySet().stream().parallel()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().finish()));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        System.out.println(measurements);
        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_artpar();
    }

    public static int parseDouble(byte[] str, int length) {

        boolean negative = false;

        int start = 0;
        int result = 0;

        // Check for negative numbers
        if (str[0] == '-') {
            negative = true;
            start++;
        }

        for (int i = start; i < length; i++) {
            byte c = str[i];

            if (c != '.') {
                result = result * 10 + (c - '0');
            }
        }

        return negative ? -result : result;
    }

    public static int hashCode(byte[] array, int length) {

        int h = 1;
        int i = 0;
        for (; i + 7 < length; i += 8) {
            h = 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * h + 31 * 31 * 31 * 31
                    * 31 * 31 * 31 * array[i] + 31 * 31 * 31 * 31 * 31 * 31
                            * array[i + 1]
                    + 31 * 31 * 31 * 31 * 31 * array[i + 2] + 31
                            * 31 * 31 * 31 * array[i + 3]
                    + 31 * 31 * 31 * array[i + 4]
                    + 31 * 31 * array[i + 5] + 31 * array[i + 6] + array[i + 7];
        }

        for (; i + 3 < length; i += 4) {
            h = 31 * 31 * 31 * 31 * h + 31 * 31 * 31 * array[i] + 31 * 31
                    * array[i + 1] + 31 * array[i + 2] + array[i + 3];
        }
        for (; i < length; i++) {
            h = 31 * h + array[i];
        }

        return h;
    }

    private record ResultRow(double min, double mean, double max, long count, double sum) {
        public String toString() {
            return round(min / 10) + "/" + round(mean / 10) + "/" + round(max / 10);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator() {
        }

        // public MeasurementAggregator(double min, double max, double sum, long count) {
        // this.min = min;
        // this.max = max;
        // this.sum = sum;
        // this.count = count;
        // }

        MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        // MeasurementAggregator combine(double otherMin, double otherMax, double otherSum, long otherCount) {
        // min = Math.min(min, otherMin);
        // max = Math.max(max, otherMax);
        // sum += otherSum;
        // count += otherCount;
        // return this;
        // }

        MeasurementAggregator combine(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count += 1;
            return this;
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max, count, sum);
        }
    }

    static class StationName {
        public final int hash;
        private final String name;
        // private final int index;
        public int count = 0;
        // public int[] values = new int[VECTOR_SIZE];
        public MeasurementAggregator measurementAggregator = new MeasurementAggregator();

        public StationName(String name, int hash) {
            this.name = name;
            // this.index = index;
            this.hash = hash;
        }

    }

    private class ReaderRunnable {
        private final MappedByteBuffer mappedByteBuffer;
        StationNameMap stationNameMap = new StationNameMap();
        // double[][] stationValueMap = new double[SIZE][];

        private ReaderRunnable(MappedByteBuffer mappedByteBuffer) {
            this.mappedByteBuffer = mappedByteBuffer;
        }

        public Map<String, MeasurementAggregator> run() {
            // System.out.println("Started future - " + mappedByteBuffer.position());

            int doubleValue;
            long start = Date.from(Instant.now()).getTime();
            // int totalBytesRead = 0;

            // ByteBuffer nameBuffer = ByteBuffer.allocate(128);
            byte[] rawBuffer = new byte[128];
            int bufferIndex = 0;
            StationName matchedStation = null;
            boolean readUntilSemiColon = true;

            int MAPPED_BYTE_BUFFER_SIZE = 1024;
            byte[] mappedBytes = new byte[MAPPED_BYTE_BUFFER_SIZE];
            int i1;
            while (mappedByteBuffer.hasRemaining()) {
                int remaining = mappedByteBuffer.remaining();
                int bytesToRead = Math.min(remaining, MAPPED_BYTE_BUFFER_SIZE);
                mappedByteBuffer.get(mappedBytes, 0, bytesToRead);
                i1 = 0;
                while (i1 < remaining && i1 < MAPPED_BYTE_BUFFER_SIZE) {
                    byte b = mappedBytes[i1];
                    i1++;
                    if (readUntilSemiColon) {
                        if (b != ';') {
                            rawBuffer[bufferIndex] = b;
                            bufferIndex++;
                        }
                        else {
                            readUntilSemiColon = false;
                            matchedStation = stationNameMap.getOrCreate(rawBuffer, bufferIndex);
                            bufferIndex = 0;
                        }
                    }
                    else if (b != '\n') {
                        rawBuffer[bufferIndex] = b;
                        bufferIndex++;
                    }
                    else {
                        readUntilSemiColon = true;

                        boolean negative = false;

                        int start1 = 0;
                        int result = 0;

                        // Check for negative numbers
                        if (rawBuffer[0] == '-') {
                            negative = true;
                            start1++;
                        }

                        for (int i = start1; i < bufferIndex; i++) {
                            byte c = rawBuffer[i];
                            if (c != '.') {
                                result = result * 10 + (c - '0');
                            }
                        }

                        doubleValue = negative ? -result : result;
                        bufferIndex = 0;
                        matchedStation.measurementAggregator.combine(doubleValue);
                        matchedStation.count++;
                    }
                }

            }

            long end = Date.from(Instant.now()).getTime();
            // System.out.println("Took [" + ((end - start) / 1000) + "s for " + totalBytesRead / 1024 + " kb");

            return Arrays.stream(stationNameMap.names).parallel().filter(Objects::nonNull)
                    .collect(Collectors.toMap(e -> e.name, e -> e.measurementAggregator));
            // return groupedMeasurements;
        }
    }

    class StationNameMap {
        int[] indexes = new int[SIZE];
        StationName[] names = new StationName[SIZE];
        int currentIndex = 0;

        public StationName getOrCreate(byte[] stationNameBytes, int length) {
            int hash = CalculateAverage_artpar.hashCode(stationNameBytes, length);
            int position = Math.abs(hash) % SIZE;
            while (indexes[position] != 0 && names[indexes[position]].hash != hash) {
                position = ++position % SIZE;
            }
            if (indexes[position] != 0) {
                return names[indexes[position]];
            }
            StationName stationName = new StationName(
                    new String(stationNameBytes, 0, length, StandardCharsets.UTF_8), hash);
            indexes[position] = ++currentIndex;
            names[indexes[position]] = stationName;
            return stationName;
        }
    }

}
