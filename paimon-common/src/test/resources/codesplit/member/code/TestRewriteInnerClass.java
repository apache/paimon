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

public class TestRewriteInnerClass {
    public int a;
    protected int b = 1;
    final int c;

    public TestRewriteInnerClass(int arg) {
        c = arg;
    }

    public int myFun(int d) {
        int aa = a;
        return a + aa + this.b + c + d;
    }

    public class InnerClass {
        public int innerA;
        protected int innerB = 1;
        final int innerC;

        public TestRewriteInnerClass(int arg) {
            this.innerC = arg;
        }

        public int myFun(int d) {
            int aa = innerA;
            return innerA + aa + this.innerB + innerC + d + a + this.b + c;
        }
    }
}
