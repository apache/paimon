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

public class TestNotRewriteMember {
    int a;
    private static int b;
    public final int c = 1;
    // not rewrite static member
    public static final int d = 2;
    private int e;
    // special varaible name used by code generators, does not rewrite this
    private Object[] references;

    public TestNotRewriteMember(Object[] references) {
        this.references = references;
    }

    public int myFun() {
        return a + b + this.c + TestNotRewriteMember.d + e;
    }
}
