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

public class TestRewriteMemberField {
    public int a;
    private long b;
    protected int c = 1;
    long d;
    final int e;
    public final String f;
    private final String g = "GGGGG";
    String h;

    public TestRewriteMemberField(String s) {
        this.f = s;
    }

    public String myFun(String h) {
        int aa = a;
        return f + aa + this.h + a + String.valueOf(c);
    }
}
