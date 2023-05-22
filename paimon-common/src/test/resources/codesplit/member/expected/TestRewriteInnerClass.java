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
int[] rewrite$0 = new int[3];

{
rewrite$0[1] = 1;
}

    
    
    

    public TestRewriteInnerClass(int arg) {
        rewrite$0[2] = arg;
    }

    public int myFun(int d) {
        int aa = rewrite$0[0];
        return rewrite$0[0] + aa + this.rewrite$0[1] + rewrite$0[2] + d;
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
            return innerA + aa + this.innerB + innerC + d + rewrite$0[0] + this.rewrite$0[1] + rewrite$0[2];
        }
    }
}
