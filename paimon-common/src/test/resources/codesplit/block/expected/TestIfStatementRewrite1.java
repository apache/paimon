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

public class TestIfStatementRewrite1 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {

        if (a[0] == 0) {
            myFun1_0_0_rewriteGroup5(a, b);
        } else {
            myFun1_0_6_rewriteGroup8(a, b);
        }
    }

    void myFun1_0_0_1_2_3_5(int[] a, int[] b) throws RuntimeException {
        a[2] = b[2];
        a[22] = b[2];
    }

    void myFun1_0_0_1_2_3_4(int[] a, int[] b) throws RuntimeException {
        a[2] = 1;
        a[22] = 1;
    }

    void myFun1_0_0_1(int[] a, int[] b) throws RuntimeException {
        System.out.println("0");
        System.out.println("0");
    }

    void myFun1_0_0_1_2_3(int[] a, int[] b) throws RuntimeException {
        System.out.println("1");
        System.out.println("2");
    }

    void myFun1_0_7_8(int[] a, int[] b) throws RuntimeException {
        System.out.println("3");
        System.out.println("3");
    }

    void myFun1_0_7_8_10(int[] a, int[] b) throws RuntimeException {
        a[0] = 2 * b[0];
        a[1] = 2 * b[1];
        a[2] = 2 * b[2];
    }

    void myFun1_0_6_rewriteGroup8(int[] a, int[] b) throws RuntimeException {
        myFun1_0_7_8(a, b);
        if (a[1] == 1) {
            myFun1_0_7_8_9(a, b);
        } else {
            myFun1_0_7_8_10(a, b);
        }
    }

    void myFun1_0_0_rewriteGroup1_2_rewriteGroup4(int[] a, int[] b) throws RuntimeException {
        myFun1_0_0_1_2_3(a, b);
        if (a[2] == 0) {
            myFun1_0_0_1_2_3_4(a, b);
        } else {
            myFun1_0_0_1_2_3_5(a, b);
        }
    }

    void myFun1_0_0_1_6(int[] a, int[] b) throws RuntimeException {
        a[1] = b[1];
        a[2] = b[2];
    }

    void myFun1_0_0_rewriteGroup5(int[] a, int[] b) throws RuntimeException {
        myFun1_0_0_1(a, b);
        if (a[1] == 0) {
            myFun1_0_0_rewriteGroup1_2_rewriteGroup4(a, b);
        } else {
            myFun1_0_0_1_6(a, b);
        }
    }

    void myFun1_0_7_8_9(int[] a, int[] b) throws RuntimeException {
        a[0] = b[0];
        a[1] = b[1];
        a[2] = b[2];
    }

}
