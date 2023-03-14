/*
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package org.apache.flink.table.store.utils;

/** A simple read buffer provides {@code readUnsignedByte} and position. */
public final class SimpleReadBuffer {

    private int pos = 0;
    private final byte[] buf;

    public SimpleReadBuffer(byte[] data) {
        buf = data;
    }

    public byte[] getBuf() {
        return buf;
    }

    public int getPos() {
        return pos;
    }

    public SimpleReadBuffer reset() {
        pos = 0;
        return this;
    }

    public int readUnsignedByte() {
        return buf[pos++] & 0xff;
    }
}
