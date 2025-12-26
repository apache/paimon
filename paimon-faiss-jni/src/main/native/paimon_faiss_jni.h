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

#ifndef PAIMON_FAISS_JNI_H
#define PAIMON_FAISS_JNI_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexFactoryCreate
 * Signature: (ILjava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexFactoryCreate
  (JNIEnv *, jclass, jint, jstring, jint);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexDestroy
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexGetDimension
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetDimension
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexGetCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetCount
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexIsTrained
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_paimon_faiss_FaissNative_indexIsTrained
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexGetMetricType
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetMetricType
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexTrain
 * Signature: (JJ[F)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexTrain
  (JNIEnv *, jclass, jlong, jlong, jfloatArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexAdd
 * Signature: (JJ[F)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAdd
  (JNIEnv *, jclass, jlong, jlong, jfloatArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexAddWithIds
 * Signature: (JJ[F[J)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAddWithIds
  (JNIEnv *, jclass, jlong, jlong, jfloatArray, jlongArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexSearch
 * Signature: (JJ[FI[F[J)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSearch
  (JNIEnv *, jclass, jlong, jlong, jfloatArray, jint, jfloatArray, jlongArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexRangeSearch
 * Signature: (JJ[FF)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexRangeSearch
  (JNIEnv *, jclass, jlong, jlong, jfloatArray, jfloat);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexRemoveIds
 * Signature: (J[J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexRemoveIds
  (JNIEnv *, jclass, jlong, jlongArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexReset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexReset
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexWriteToFile
 * Signature: (JLjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexWriteToFile
  (JNIEnv *, jclass, jlong, jstring);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexReadFromFile
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexReadFromFile
  (JNIEnv *, jclass, jstring);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexSerialize
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSerialize
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    indexDeserialize
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexDeserialize
  (JNIEnv *, jclass, jbyteArray);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    rangeSearchResultDestroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultDestroy
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    rangeSearchResultGetLimits
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLimits
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    rangeSearchResultGetLabels
 * Signature: (J)[J
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLabels
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    rangeSearchResultGetDistances
 * Signature: (J)[F
 */
JNIEXPORT jfloatArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetDistances
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    ivfGetNprobe
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfGetNprobe
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    ivfSetNprobe
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfSetNprobe
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    ivfGetNlist
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfGetNlist
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    hnswGetEfSearch
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswGetEfSearch
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    hnswSetEfSearch
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswSetEfSearch
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    hnswGetEfConstruction
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswGetEfConstruction
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    getVersion
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_paimon_faiss_FaissNative_getVersion
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    setNumThreads
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_setNumThreads
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_paimon_faiss_FaissNative
 * Method:    getNumThreads
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_getNumThreads
  (JNIEnv *, jclass);

#ifdef __cplusplus
}
#endif

#endif /* PAIMON_FAISS_JNI_H */

