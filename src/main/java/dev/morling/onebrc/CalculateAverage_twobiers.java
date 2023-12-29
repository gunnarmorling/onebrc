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

import static java.util.stream.Collectors.averagingDouble;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CalculateAverage_twobiers {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        Map<String, Double> measurements = Files.lines(Paths.get(FILE))
                .parallel()
                .map(l -> fastSplit(l))
                .collect(groupingBy(m -> m[0], averagingDouble(m -> fastParseDouble(m[1]))));

        measurements = new TreeMap<>(measurements.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> Math.round(e.getValue() * 10.0) / 10.0)));

        System.out.println(measurements);
    }

    private static String[] fastSplit(String str) {
        var splitArray = new String[2];
        var chars = str.toCharArray();

        int i = 0;
        for (char c : chars) {
            if (c == ';') {
                splitArray[0] = new String(Arrays.copyOfRange(chars, 0, i));
                break;
            }
            i++;
        }

        splitArray[1] = new String(Arrays.copyOfRange(chars, i + 1, chars.length));
        return splitArray;
    }

    private static Double fastParseDouble(String str) {
        long value = 0;
        int exp = 0;
        boolean negative = false;
        int decimalPlaces = Integer.MIN_VALUE;
        var chars = str.toCharArray();
        for (char c : chars) {
            if (c >= '0' && c <= '9') {
                value = value * 10 + (c - '0');
                decimalPlaces++;
            }
            else if (c == '-') {
                negative = true;
            }
            else if (c == '.') {
                decimalPlaces = 0;
            }
        }

        return asDouble(value, exp, negative, decimalPlaces);
    }

    private static double asDouble(long value, int exp, boolean negative, int decimalPlaces) {
        if (decimalPlaces > 0 && value < Long.MAX_VALUE / 2) {
            if (value < Long.MAX_VALUE / (1L << 32)) {
                exp -= 32;
                value <<= 32;
            }
            if (value < Long.MAX_VALUE / (1L << 16)) {
                exp -= 16;
                value <<= 16;
            }
            if (value < Long.MAX_VALUE / (1L << 8)) {
                exp -= 8;
                value <<= 8;
            }
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
            }
        }
        for (; decimalPlaces > 0; decimalPlaces--) {
            exp--;
            long mod = value % 5;
            value /= 5;
            int modDiv = 1;
            if (value < Long.MAX_VALUE / (1L << 4)) {
                exp -= 4;
                value <<= 4;
                modDiv <<= 4;
            }
            if (value < Long.MAX_VALUE / (1L << 2)) {
                exp -= 2;
                value <<= 2;
                modDiv <<= 2;
            }
            if (value < Long.MAX_VALUE / (1L << 1)) {
                exp -= 1;
                value <<= 1;
                modDiv <<= 1;
            }
            if (decimalPlaces > 1)
                value += modDiv * mod / 5;
            else
                value += (modDiv * mod + 4) / 5;
        }
        final double d = Math.scalb((double) value, exp);
        return negative ? -d : d;
    }
}
