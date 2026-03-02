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

#ifndef PAIMON_LUMINA_JNI_H
#define PAIMON_LUMINA_JNI_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

// ==================== Builder API ====================

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderCreate
  (JNIEnv *, jclass, jobjectArray, jobjectArray);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderPretrain
  (JNIEnv *, jclass, jlong, jobject, jlong);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderInsertBatch
  (JNIEnv *, jclass, jlong, jobject, jobject, jlong);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderDump
  (JNIEnv *, jclass, jlong, jstring);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderDestroy
  (JNIEnv *, jclass, jlong);

// ==================== Searcher API ====================

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherCreate
  (JNIEnv *, jclass, jobjectArray, jobjectArray);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherOpen
  (JNIEnv *, jclass, jlong, jstring);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherSearch
  (JNIEnv *, jclass, jlong, jfloatArray, jint, jint, jfloatArray, jlongArray,
   jobjectArray, jobjectArray);

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherGetCount
  (JNIEnv *, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherGetDimension
  (JNIEnv *, jclass, jlong);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherClose
  (JNIEnv *, jclass, jlong);

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherDestroy
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif

#endif // PAIMON_LUMINA_JNI_H
