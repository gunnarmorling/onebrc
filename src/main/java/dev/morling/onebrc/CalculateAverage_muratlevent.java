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
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

public class CalculateAverage_muratlevent {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) {
        try {
            Map<String, DoubleSummaryStatistics> stationMeasurements = Files.lines(Paths.get(FILE))
                    .parallel()
                    .map(line -> line.split(";"))
                    .collect(Collectors.groupingBy(
                            parts -> parts[0],
                            Collectors.summarizingDouble(parts -> Double.parseDouble(parts[1]))));

            stationMeasurements.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> System.out.println(entry.getKey() + "=" + format(entry.getValue())));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String format(DoubleSummaryStatistics stats) {
        return String.format("%.1f/%.1f/%.1f", stats.getMin(), stats.getAverage(), stats.getMax());
    }
}
