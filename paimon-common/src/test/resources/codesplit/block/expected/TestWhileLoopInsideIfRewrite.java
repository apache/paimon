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

public class TestWhileLoopInsideIfRewrite {

    int counter = 0;

    public void myFun(int[] a, int[] b, int[] c) {

        a[0] += b[1];
        b[1] += a[1];

        if (a.length < 100) {
            while (counter < 10) {
                myFun_0_0_1_2_3(a, b, c);
                if (a[counter] > 0) {
                    myFun_0_0_1_2_3_4(a, b, c);
                } else {
                    myFun_0_0_1_2_3_5(a, b, c);
                }

                myFun_0_0_1_2_6(a, b, c);
                if (a[0] > 0) {
                    System.out.println("Hello");
                } else {
                    System.out.println("World");
                }

                myFun_0_0_1_2(a, b, c);
            }
        } else {
            myFun_0_0_rewriteGroup2(a, b, c);

            myFun_0_9(a, b, c);
        }

        a[4] += b[4];
        b[5] += a[5];
    }

    void myFun_0_0_rewriteGroup2(int[] a, int[] b, int[] c) {
        while (counter < 10) {
            myFun_0_9_10_11(a, b, c);
        }
    }

    void myFun_0_0_1_2(int[] a, int[] b, int[] c) {
        counter--;
        System.out.println("World ffff");
    }

    void myFun_0_0_1_2_6(int[] a, int[] b, int[] c) {
        a[2] += b[2];
        b[3] += a[3];
    }

    void myFun_0_9_10_11(int[] a, int[] b, int[] c) {
        b[counter] = b[counter]++;
        counter++;
    }

    void myFun_0_0_1_2_3_5(int[] a, int[] b, int[] c) {
        b[counter] = a[counter] * 3;
        System.out.println(b[counter]);
    }

    void myFun_0_9(int[] a, int[] b, int[] c) {
        System.out.println("World Else");
        System.out.println("World Else 2");
    }

    void myFun_0_0_1_2_3(int[] a, int[] b, int[] c) {
        c[counter] = a[0] + 1000;
        System.out.println(c);
    }

    void myFun_0_0_1_2_3_4(int[] a, int[] b, int[] c) {
        b[counter] = a[counter] * 2;
        c[counter] = b[counter] * 2;
        System.out.println(b[counter]);
    }

}
