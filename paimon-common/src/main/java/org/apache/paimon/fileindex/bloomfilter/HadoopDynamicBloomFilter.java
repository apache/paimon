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

package org.apache.paimon.fileindex.bloomfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.bloom.Key;

/* This file is based on source code from the Hadoop Project (https://hadoop.apache.org//), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Mostly copied from hadoop DynamicBloomFilter. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HadoopDynamicBloomFilter extends HadoopFilter {

    // Every next floor of bloom filter expand factor
    private static final int EXPANSION_FACTOR = 20;

    /** Threshold for the maximum number of key to record in a dynamic Bloom filter row. */
    private int nr;

    /** The number of keys recorded in the current standard active Bloom filter. */
    private int currentNbRecord;

    /** The matrix of Bloom filter. */
    private HadoopBloomFilter[] matrix;

    /** Zero-args constructor for the serialization. */
    public HadoopDynamicBloomFilter() {}

    /**
     * Constructor.
     *
     * <p>Builds an empty Dynamic Bloom filter.
     *
     * @param vectorSize The number of bits in the vector.
     * @param nbHash The number of hash function to consider.
     * @param hashType type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
     * @param nr The threshold for the maximum number of keys to record in a dynamic Bloom filter
     *     row.
     */
    public HadoopDynamicBloomFilter(int vectorSize, int nbHash, int hashType, int nr) {
        super(vectorSize, nbHash, hashType);

        this.nr = nr;
        this.currentNbRecord = 0;

        matrix = new HadoopBloomFilter[1];
        matrix[0] = new HadoopBloomFilter(this.vectorSize, this.nbHash, this.hashType);
    }

    @Override
    public boolean add(Key key) {
        if (key == null) {
            throw new NullPointerException("Key can not be null");
        }

        HadoopBloomFilter bf = getActiveStandardBF();

        // get or advance
        if (bf == null) {
            addRow();
            bf = matrix[matrix.length - 1];
            currentNbRecord = 0;
        }

        if (bf.add(key)) {
            currentNbRecord++;
            return true;
        }

        return false;
    }

    @Override
    public boolean addHash(long hash64) {
        HadoopBloomFilter bf = getActiveStandardBF();

        // get or advance
        if (bf == null) {
            addRow();
            bf = matrix[matrix.length - 1];
            currentNbRecord = 0;
        }

        if (bf.addHash(hash64)) {
            currentNbRecord++;
            return true;
        }

        return false;
    }

    @Override
    public void and(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopDynamicBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be and-ed");
        }

        HadoopDynamicBloomFilter dbf = (HadoopDynamicBloomFilter) filter;

        if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
            throw new IllegalArgumentException("filters cannot be and-ed");
        }

        for (int i = 0; i < matrix.length; i++) {
            matrix[i].and(dbf.matrix[i]);
        }
    }

    @Override
    public boolean membershipTest(Key key) {
        if (key == null) {
            return true;
        }

        for (int i = 0; i < matrix.length; i++) {
            if (matrix[i].membershipTest(key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean membershipTest(long hash64) {
        for (int i = 0; i < matrix.length; i++) {
            if (matrix[i].membershipTest(hash64)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void not() {
        for (int i = 0; i < matrix.length; i++) {
            matrix[i].not();
        }
    }

    @Override
    public void or(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopDynamicBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be or-ed");
        }

        HadoopDynamicBloomFilter dbf = (HadoopDynamicBloomFilter) filter;

        if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
            throw new IllegalArgumentException("filters cannot be or-ed");
        }
        for (int i = 0; i < matrix.length; i++) {
            matrix[i].or(dbf.matrix[i]);
        }
    }

    @Override
    public void xor(HadoopFilter filter) {
        if (filter == null
                || !(filter instanceof HadoopDynamicBloomFilter)
                || filter.vectorSize != this.vectorSize
                || filter.nbHash != this.nbHash) {
            throw new IllegalArgumentException("filters cannot be xor-ed");
        }
        HadoopDynamicBloomFilter dbf = (HadoopDynamicBloomFilter) filter;

        if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
            throw new IllegalArgumentException("filters cannot be xor-ed");
        }

        for (int i = 0; i < matrix.length; i++) {
            matrix[i].xor(dbf.matrix[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < matrix.length; i++) {
            res.append(matrix[i]);
            res.append(Character.LINE_SEPARATOR);
        }
        return res.toString();
    }

    // Writable

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(nr);
        out.writeInt(currentNbRecord);
        out.writeInt(matrix.length);
        for (int i = 0; i < matrix.length; i++) {
            matrix[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        nr = in.readInt();
        currentNbRecord = in.readInt();
        int len = in.readInt();
        matrix = new HadoopBloomFilter[len];
        for (int i = 0; i < matrix.length; i++) {
            matrix[i] = new HadoopBloomFilter();
            matrix[i].readFields(in);
        }
    }

    /** Adds a new row to <i>this</i> dynamic Bloom filter. */
    private void addRow() {
        HadoopBloomFilter[] tmp = new HadoopBloomFilter[matrix.length + 1];

        for (int i = 0; i < matrix.length; i++) {
            tmp[i] = matrix[i];
        }

        // grow up to contain more data
        vectorSize *= EXPANSION_FACTOR;
        nr *= EXPANSION_FACTOR;

        tmp[tmp.length - 1] = new HadoopBloomFilter(vectorSize, nbHash, hashType);

        matrix = tmp;
    }

    /**
     * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
     *
     * @return BloomFilter The active standard Bloom filter. <code>Null</code> otherwise.
     */
    private HadoopBloomFilter getActiveStandardBF() {
        if (currentNbRecord >= nr) {
            return null;
        }

        return matrix[matrix.length - 1];
    }
}
