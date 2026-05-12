/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.mosaic;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Byte Pair Encoding (BPE) for column name compression. Tokens use bytes 0x80-0xFF, leaving
 * 0x00-0x7F for ASCII literals. Only applicable when all column names are ASCII-only.
 */
public class MosaicBpe {

    private static final int TOKEN_BASE = 0x80;
    private static final int MAX_RULES = 128;

    public static boolean isAsciiOnly(byte[][] names) {
        for (byte[] name : names) {
            for (byte b : name) {
                if ((b & 0x80) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public static byte[][] buildVocabulary(byte[][] names) {
        // Work on mutable copies
        int[][] tokens = new int[names.length][];
        for (int i = 0; i < names.length; i++) {
            tokens[i] = new int[names[i].length];
            for (int j = 0; j < names[i].length; j++) {
                tokens[i][j] = names[i][j] & 0xFF;
            }
        }

        byte[][] rules = new byte[MAX_RULES][2];
        int numRules = 0;

        for (int r = 0; r < MAX_RULES; r++) {
            // Count all adjacent token pairs
            Map<Long, int[]> pairCounts = new HashMap<>();
            for (int[] seq : tokens) {
                for (int j = 0; j < seq.length - 1; j++) {
                    long pair = ((long) seq[j] << 16) | seq[j + 1];
                    pairCounts.computeIfAbsent(pair, k -> new int[1])[0]++;
                }
            }

            // Find most frequent pair
            long bestPair = -1;
            int bestCount = 1;
            for (Map.Entry<Long, int[]> e : pairCounts.entrySet()) {
                if (e.getValue()[0] > bestCount) {
                    bestCount = e.getValue()[0];
                    bestPair = e.getKey();
                }
            }

            if (bestPair == -1) {
                break;
            }

            int left = (int) (bestPair >>> 16);
            int right = (int) (bestPair & 0xFFFF);
            int newToken = TOKEN_BASE + numRules;
            rules[numRules][0] = (byte) left;
            rules[numRules][1] = (byte) right;
            numRules++;

            // Replace all occurrences of (left, right) with newToken
            for (int i = 0; i < tokens.length; i++) {
                tokens[i] = replacePair(tokens[i], left, right, newToken);
            }
        }

        byte[][] result = new byte[numRules][2];
        System.arraycopy(rules, 0, result, 0, numRules);
        return result;
    }

    private static int[] replacePair(int[] seq, int left, int right, int newToken) {
        // Count replacements with same skip logic as actual replacement
        int count = 0;
        int j = 0;
        while (j < seq.length) {
            if (j < seq.length - 1 && seq[j] == left && seq[j + 1] == right) {
                count++;
                j += 2;
            } else {
                j++;
            }
        }
        if (count == 0) {
            return seq;
        }

        int[] result = new int[seq.length - count];
        int pos = 0;
        j = 0;
        while (j < seq.length) {
            if (j < seq.length - 1 && seq[j] == left && seq[j + 1] == right) {
                result[pos++] = newToken;
                j += 2;
            } else {
                result[pos++] = seq[j++];
            }
        }
        return result;
    }

    public static byte[] encode(byte[] name, byte[][] rules) {
        int[] tokens = new int[name.length];
        for (int i = 0; i < name.length; i++) {
            tokens[i] = name[i] & 0xFF;
        }

        for (int r = 0; r < rules.length; r++) {
            int left = rules[r][0] & 0xFF;
            int right = rules[r][1] & 0xFF;
            int newToken = TOKEN_BASE + r;
            tokens = replacePair(tokens, left, right, newToken);
        }

        byte[] result = new byte[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            result[i] = (byte) tokens[i];
        }
        return result;
    }

    public static byte[] decode(byte[] encoded, byte[][] rules) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(encoded.length * 2);
        for (byte b : encoded) {
            expand(b & 0xFF, rules, out);
        }
        return out.toByteArray();
    }

    private static void expand(int token, byte[][] rules, ByteArrayOutputStream out) {
        if (token < TOKEN_BASE) {
            out.write(token);
        } else {
            int idx = token - TOKEN_BASE;
            expand(rules[idx][0] & 0xFF, rules, out);
            expand(rules[idx][1] & 0xFF, rules, out);
        }
    }
}
