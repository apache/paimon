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
    public void myFun(int[] a, int[] b) {

        if (a[0] == 0) {
            myFun_0_0(a, b);
        } else {
            myFun_0_1(a, b);
        }
    }

    void myFun_0_1(int[] a, int[] b) {
        a[0] += b[1];
        a[1] += b[0];
    }

    void myFun_0_0(int[] a, int[] b) {
        a[0] += b[0];
        a[1] += b[1];
    }


    public class InnerClass {
        public void myFun(int[] a, int[] b) {

            if (a[0] == 0) {
                myFun_0_0(a, b);
            } else {
                myFun_0_1(a, b);
            }
        }

        void myFun_0_1(int[] a, int[] b) {
            a[0] += b[1];
            a[1] += b[0];
        }

        void myFun_0_0(int[] a, int[] b) {
            a[0] += b[0];
            a[1] += b[1];
        }

    }
}
