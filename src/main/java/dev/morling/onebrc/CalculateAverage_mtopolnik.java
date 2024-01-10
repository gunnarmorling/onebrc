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

import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.TreeMap;

public class CalculateAverage_mtopolnik {
    private static final Unsafe UNSAFE = unsafe();
    private static final boolean ORDER_IS_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    private static final int MAX_NAME_LEN = 100;
    private static final int STATS_TABLE_SIZE = 1 << 16;
    private static final int TABLE_INDEX_MASK = STATS_TABLE_SIZE - 1;
    private static final String MEASUREMENTS_TXT = "measurements.txt";
    private static final byte SEMICOLON = ';';
    private static final long BROADCAST_SEMICOLON = broadcastByte(SEMICOLON);

    // These two are just informative, I let the IDE calculate them for me
    private static final long NATIVE_MEM_PER_THREAD = StatsAccessor.SIZEOF * STATS_TABLE_SIZE;
    private static final long NATIVE_MEM_ON_8_THREADS = 8 * NATIVE_MEM_PER_THREAD;

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static class StationStats {
        String name;
        long sum;
        int count;
        int min;
        int max;

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }
    }

    public static void main(String[] args) throws Exception {
        calculate();
    }

    static void calculate() throws Exception {
        final File file = new File(MEASUREMENTS_TXT);
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final var results = new StationStats[(int) chunkCount][];
        final var chunkStartOffsets = new long[chunkCount];
        try (var raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }
            var threads = new Thread[(int) chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(raf, chunkStart, chunkLimit, results, i));
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        }
        var totals = new TreeMap<String, StationStats>();
        for (var chunkResults : results) {
            for (var stats : chunkResults) {
                var prev = totals.putIfAbsent(stats.name, stats);
                if (prev != null) {
                    prev.sum += stats.sum;
                    prev.count += stats.count;
                    prev.min = Integer.min(prev.min, stats.min);
                    prev.max = Integer.max(prev.max, stats.max);
                }
            }
        }
        System.out.println(totals);
    }

    private static class ChunkProcessor implements Runnable {
        private static final long HASHBUF_SIZE = 2 * Long.BYTES;
        private static final int CACHELINE_SIZE = 64;

        private final long chunkStart;
        private final long chunkLimit;
        private final RandomAccessFile raf;
        private final StationStats[][] results;
        private final int myIndex;

        private StatsAccessor stats;
        private long inputBase;
        private long inputSize;
        private long hashBufBase;
        private long cursor;

        ChunkProcessor(RandomAccessFile raf, long chunkStart, long chunkLimit, StationStats[][] results, int myIndex) {
            this.raf = raf;
            this.chunkStart = chunkStart;
            this.chunkLimit = chunkLimit;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            try (Arena confinedArena = Arena.ofConfined()) {
                final var inputMem = raf.getChannel().map(MapMode.READ_ONLY, chunkStart, chunkLimit - chunkStart, confinedArena);
                inputBase = inputMem.address();
                inputSize = inputMem.byteSize();
                stats = new StatsAccessor(confinedArena.allocate(STATS_TABLE_SIZE * StatsAccessor.SIZEOF, CACHELINE_SIZE));
                hashBufBase = confinedArena.allocate(HASHBUF_SIZE).address();
                processChunk();
                exportResults();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void processChunk() {
            while (cursor < inputSize) {
                long posOfSemicolon = posOfSemicolon();
                long hash = hash(posOfSemicolon);
                long namePos = cursor;
                long nameLen = posOfSemicolon - cursor;
                assert nameLen <= 100 : "nameLen > 100";
                int temperature = parseTemperatureAndAdvanceCursor(posOfSemicolon);
                updateStats(hash, namePos, nameLen, temperature);
            }
        }

        private void updateStats(long hash, long namePos, long nameLen, int temperature) {
            int tableIndex = (int) (hash & TABLE_INDEX_MASK);
            while (true) {
                stats.gotoIndex(tableIndex);
                long foundHash = stats.hash();
                if (foundHash == hash && stats.nameLen() == nameLen
                        && nameEquals(stats.nameAddress(), inputBase + namePos, nameLen)) {
                    stats.setSum(stats.sum() + temperature);
                    stats.setCount(stats.count() + 1);
                    stats.setMin((short) Integer.min(stats.min(), temperature));
                    stats.setMax((short) Integer.max(stats.max(), temperature));
                    return;
                }
                if (foundHash != 0) {
                    tableIndex = (tableIndex + 1) & TABLE_INDEX_MASK;
                    continue;
                }
                stats.setHash(hash);
                stats.setNameLen((int) nameLen);
                stats.setSum(temperature);
                stats.setCount(1);
                stats.setMin((short) temperature);
                stats.setMax((short) temperature);
                UNSAFE.copyMemory(inputBase + namePos, stats.nameAddress(), nameLen);
                return;
            }
        }

        private int parseTemperatureAndAdvanceCursor(long semicolonPos) {
            long startOffset = semicolonPos + 1;
            if (startOffset <= inputSize - Long.BYTES) {
                return parseTemperatureSwarAndAdvanceCursor(startOffset);
            }
            return parseTemperatureSimpleAndAdvanceCursor(startOffset);
        }

        // Credit: merykitty
        private int parseTemperatureSwarAndAdvanceCursor(long startOffset) {
            long word = UNSAFE.getLong(inputBase + startOffset);
            if (ORDER_IS_BIG_ENDIAN) {
                word = Long.reverseBytes(word);
            }
            final long negated = ~word;
            final int dotPos = Long.numberOfTrailingZeros(negated & 0x10101000);
            final long signed = (negated << 59) >> 63;
            final long removeSignMask = ~(signed & 0xFF);
            final long digits = ((word & removeSignMask) << (28 - dotPos)) & 0x0F000F0F00L;
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final int temperature = (int) ((absValue ^ signed) - signed);
            cursor = startOffset + (dotPos / 8) + 3;
            return temperature;
        }

        private int parseTemperatureSimpleAndAdvanceCursor(long startOffset) {
            final byte minus = (byte) '-';
            final byte zero = (byte) '0';
            final byte dot = (byte) '.';

            // Temperature plus the following newline is at least 4 chars, so this is always safe:
            int fourCh = UNSAFE.getInt(inputBase + startOffset);
            if (ORDER_IS_BIG_ENDIAN) {
                fourCh = Integer.reverseBytes(fourCh);
            }
            final int mask = 0xFF;
            byte ch = (byte) (fourCh & mask);
            int shift = 0;
            int temperature;
            int sign;
            if (ch == minus) {
                sign = -1;
                shift += 8;
                ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            }
            else {
                sign = 1;
            }
            temperature = ch - zero;
            shift += 8;
            ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            if (ch == dot) {
                shift += 8;
                ch = (byte) ((fourCh & (mask << shift)) >>> shift);
            }
            else {
                temperature = 10 * temperature + (ch - zero);
                shift += 16;
                // The last character may be past the four loaded bytes, load it from memory.
                // Checking that with another `if` is self-defeating for performance.
                ch = UNSAFE.getByte(inputBase + startOffset + (shift / 8));
            }
            temperature = 10 * temperature + (ch - zero);
            // `shift` holds the number of bits in the temperature field.
            // A newline character follows the temperature, and so we advance
            // the cursor past the newline to the start of the next line.
            cursor = startOffset + (shift / 8) + 2;
            return sign * temperature;
        }

        private long hash(long posOfSemicolon) {
            long n1, n2;
            if (cursor <= inputSize - HASHBUF_SIZE) {
                long offset = cursor;
                n1 = UNSAFE.getLong(inputBase + offset);
                if (ORDER_IS_BIG_ENDIAN) {
                    n1 = Long.reverseBytes(n1);
                }
                // Mask out bytes not belonging to name
                n1 = maskWord(n1, posOfSemicolon - offset);

//                offset += Long.BYTES;
//                n2 = UNSAFE.getLong(inputBase + offset);
//                if (ORDER_IS_BIG_ENDIAN) {
//                    n2 = Long.reverseBytes(n2);
//                }
//                nameSize = Long.max(0, posOfSemicolon - offset);
//                n2 = maskWord(n2, nameSize);
            }
            else {
                UNSAFE.putLong(hashBufBase, 0);
                // UNSAFE.putLong(hashBufBase + Long.BYTES, 0);
                UNSAFE.copyMemory(inputBase + cursor, hashBufBase, Long.min(HASHBUF_SIZE, posOfSemicolon - cursor));
                n1 = UNSAFE.getLong(hashBufBase);
                // n2 = UNSAFE.getLong(hashBufBase + Long.BYTES);
                if (ORDER_IS_BIG_ENDIAN) {
                    n1 = Long.reverseBytes(n1);
                    // n2 = Long.reverseBytes(n2);
                }
            }
            long seed = 0x51_7c_c1_b7_27_22_0a_95L;
            int rotDist = 17;
            long hash = n1;
            hash *= seed;
            hash = Long.rotateLeft(hash, rotDist);
            // hash ^= n2;
            // hash *= seed;
            // hash = Long.rotateLeft(hash, rotDist);
            hash &= (~Long.MIN_VALUE); // make hash positive
            return hash != 0 ? hash : 1;
        }

        private boolean nameEquals(long statsAddr, long inputAddr, long len) {
            int i = 0;
            if (inputAddr + 2 * Long.BYTES <= inputBase + inputSize) {
                boolean mismatch1 = wordMismatch(inputAddr, statsAddr, len);
                boolean mismatch2 = wordMismatch(inputAddr + Long.BYTES, statsAddr + Long.BYTES, len - Long.BYTES);
                if (mismatch1 | mismatch2) {
                    return false;
                }
                i = 2 * Long.BYTES;
            }
            for (; i < len; i++) {
                if (UNSAFE.getByte(statsAddr + i) != UNSAFE.getByte(inputAddr + i)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean wordMismatch(long inputAddr, long statsAddr, long len) {
            long inputWord = maskWord(UNSAFE.getLong(inputAddr), len);
            long statsWord = UNSAFE.getLong(statsAddr);
            // System.err.println("Compare '" + longToString(inputWord) + "' and '" + longToString(statsWord) + "'");
            return inputWord != statsWord;
        }

        private static long maskWord(long word, long len) {
            long halfShiftDistance = 4 * Long.max(0, Long.BYTES - len);
            long mask = (~0L >>> halfShiftDistance) >>> halfShiftDistance; // avoid Java trap of shiftDist % 64
            return word & mask;
        }

        private static final long BROADCAST_0x01 = broadcastByte(0x01);
        private static final long BROADCAST_0x80 = broadcastByte(0x80);

        // Adapted from https://jameshfisher.com/2017/01/24/bitwise-check-for-zero-byte/
        // and https://github.com/ashvardanian/StringZilla/blob/14e7a78edcc16b031c06b375aac1f66d8f19d45a/stringzilla/stringzilla.h#L139-L169
        long posOfSemicolon() {
            long offset = cursor;
            for (; offset <= inputSize - Long.BYTES; offset += Long.BYTES) {
                var block = UNSAFE.getLong(inputBase + offset);
                if (ORDER_IS_BIG_ENDIAN) {
                    block = Long.reverseBytes(block);
                }
                final long diff = block ^ BROADCAST_SEMICOLON;
                long matchIndicators = (diff - BROADCAST_0x01) & ~diff & BROADCAST_0x80;
                if (matchIndicators != 0) {
                    return offset + Long.numberOfTrailingZeros(matchIndicators) / 8;
                }
            }
            return posOfSemicolonSimple(offset);
        }

        private long posOfSemicolonSimple(long offset) {
            for (; offset < inputSize; offset++) {
                if (UNSAFE.getByte(inputBase + offset) == SEMICOLON) {
                    return offset;
                }
            }
            throw new RuntimeException("Semicolon not found");
        }

        // Copies the results from native memory to Java heap and puts them into the results array.
        private void exportResults() {
            var exportedStats = new ArrayList<StationStats>(10_000);
            for (int i = 0; i < STATS_TABLE_SIZE; i++) {
                stats.gotoIndex(i);
                if (stats.hash() == 0) {
                    continue;
                }
                var sum = stats.sum();
                var count = stats.count();
                var min = stats.min();
                var max = stats.max();
                var name = stats.exportNameString();
                var stationStats = new StationStats();
                stationStats.name = name;
                stationStats.sum = sum;
                stationStats.count = count;
                stationStats.min = min;
                stationStats.max = max;
                exportedStats.add(stationStats);
            }
            results[myIndex] = exportedStats.toArray(new StationStats[0]);
        }

        private final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());

        private String longToString(long word) {
            buf.clear();
            buf.putLong(word);
            return new String(buf.array(), StandardCharsets.UTF_8); // + "|" + Arrays.toString(buf.array());
        }
    }

    private static long broadcastByte(int b) {
        long nnnnnnnn = b;
        nnnnnnnn |= nnnnnnnn << 8;
        nnnnnnnn |= nnnnnnnn << 16;
        nnnnnnnn |= nnnnnnnn << 32;
        return nnnnnnnn;
    }

    static class StatsAccessor {
        static final int NAME_SLOT_SIZE = 104;
        static final long HASH_OFFSET = 0;
        static final long NAMELEN_OFFSET = HASH_OFFSET + Long.BYTES;
        static final long SUM_OFFSET = NAMELEN_OFFSET + Integer.BYTES;
        static final long COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final long MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final long MAX_OFFSET = MIN_OFFSET + Short.BYTES;
        static final long NAME_OFFSET = MAX_OFFSET + Short.BYTES;
        static final long SIZEOF = (NAME_OFFSET + NAME_SLOT_SIZE - 1) / 8 * 8 + 8;

        static final int ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

        private final long address;
        private long slotBase;

        StatsAccessor(MemorySegment memSeg) {
            memSeg.fill((byte) 0);
            this.address = memSeg.address();
        }

        void gotoIndex(int index) {
            slotBase = address + index * SIZEOF;
        }

        long hash() {
            return UNSAFE.getLong(slotBase + HASH_OFFSET);
        }

        int nameLen() {
            return UNSAFE.getInt(slotBase + NAMELEN_OFFSET);
        }

        int sum() {
            return UNSAFE.getInt(slotBase + SUM_OFFSET);
        }

        int count() {
            return UNSAFE.getInt(slotBase + COUNT_OFFSET);
        }

        short min() {
            return UNSAFE.getShort(slotBase + MIN_OFFSET);
        }

        short max() {
            return UNSAFE.getShort(slotBase + MAX_OFFSET);
        }

        long nameAddress() {
            return slotBase + NAME_OFFSET;
        }

        String exportNameString() {
            final var bytes = new byte[nameLen()];
            UNSAFE.copyMemory(null, nameAddress(), bytes, ARRAY_BASE_OFFSET, nameLen());
            return new String(bytes, StandardCharsets.UTF_8);
        }

        void setHash(long hash) {
            UNSAFE.putLong(slotBase + HASH_OFFSET, hash);
        }

        void setNameLen(int nameLen) {
            UNSAFE.putInt(slotBase + NAMELEN_OFFSET, nameLen);
        }

        void setSum(int sum) {
            UNSAFE.putInt(slotBase + SUM_OFFSET, sum);
        }

        void setCount(int count) {
            UNSAFE.putInt(slotBase + COUNT_OFFSET, count);
        }

        void setMin(short min) {
            UNSAFE.putShort(slotBase + MIN_OFFSET, min);
        }

        void setMax(short max) {
            UNSAFE.putShort(slotBase + MAX_OFFSET, max);
        }
    }
}
