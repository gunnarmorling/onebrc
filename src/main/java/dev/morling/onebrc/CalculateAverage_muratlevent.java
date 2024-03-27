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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CalculateAverage_muratlevent {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
    }

    private static record ResultRow(double min, double mean, double max) {
        @Override
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public void add(Measurement measurement) {
            double value = measurement.value();
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        public ResultRow finish() {
            return new ResultRow(min, sum / count, max);
        }
    }

    public static void main(String[] args) {
        ForkJoinPool customThreadPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        try {
            Map<String, MeasurementAggregator> stationMeasurements = new ConcurrentHashMap<>();
            customThreadPool.submit(() -> {
                try (var lines = Files.lines(Paths.get(FILE))) {
                    lines.parallel()
                            .map(line -> line.split(";"))
                            .map(parts -> new Measurement(parts[0], Double.parseDouble(parts[1])))
                            .forEach(measurement -> stationMeasurements.computeIfAbsent(
                                    measurement.station(),
                                    k -> new MeasurementAggregator()).add(measurement));
                }
                catch (IOException e) {
                    throw new RuntimeException("Error reading file", e);
                }
            }).get();

            stationMeasurements.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + "=" + entry.getValue().finish())
                    .forEach(System.out::println);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            customThreadPool.shutdown();
        }
    }
}
