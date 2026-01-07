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

#include "paimon_faiss_jni.h"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIDMap.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#include <faiss/impl/io.h>

#ifdef _OPENMP
#include <omp.h>
#endif

#include <cstring>
#include <string>
#include <sstream>
#include <stdexcept>
#include <vector>

// Helper macro for exception handling
#define FAISS_TRY try {
#define FAISS_CATCH(env) \
    } catch (const std::exception& e) { \
        jclass exceptionClass = env->FindClass("org/apache/paimon/faiss/FaissException"); \
        if (exceptionClass != nullptr) { \
            env->ThrowNew(exceptionClass, e.what()); \
        } \
    } catch (...) { \
        jclass exceptionClass = env->FindClass("org/apache/paimon/faiss/FaissException"); \
        if (exceptionClass != nullptr) { \
            env->ThrowNew(exceptionClass, "Unknown native exception"); \
        } \
    }

// Helper function to convert jstring to std::string
static std::string jstringToString(JNIEnv* env, jstring jstr) {
    if (jstr == nullptr) {
        return "";
    }
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
}

// Helper function to get index pointer from handle
static faiss::Index* getIndex(jlong handle) {
    return reinterpret_cast<faiss::Index*>(handle);
}

// Helper to get IVF index
static faiss::IndexIVF* getIndexIVF(jlong handle) {
    faiss::Index* index = getIndex(handle);
    
    // Try direct cast
    faiss::IndexIVF* ivf = dynamic_cast<faiss::IndexIVF*>(index);
    if (ivf != nullptr) {
        return ivf;
    }
    
    // Try through IDMap wrapper
    faiss::IndexIDMap* idmap = dynamic_cast<faiss::IndexIDMap*>(index);
    if (idmap != nullptr) {
        ivf = dynamic_cast<faiss::IndexIVF*>(idmap->index);
        if (ivf != nullptr) {
            return ivf;
        }
    }
    
    throw std::runtime_error("Index is not an IVF index");
}

// Helper to get HNSW index
static faiss::IndexHNSW* getIndexHNSW(jlong handle) {
    faiss::Index* index = getIndex(handle);
    
    // Try direct cast
    faiss::IndexHNSW* hnsw = dynamic_cast<faiss::IndexHNSW*>(index);
    if (hnsw != nullptr) {
        return hnsw;
    }
    
    // Try through IDMap wrapper
    faiss::IndexIDMap* idmap = dynamic_cast<faiss::IndexIDMap*>(index);
    if (idmap != nullptr) {
        hnsw = dynamic_cast<faiss::IndexHNSW*>(idmap->index);
        if (hnsw != nullptr) {
            return hnsw;
        }
    }
    
    throw std::runtime_error("Index is not an HNSW index");
}

// Range search result wrapper
struct RangeSearchResultWrapper {
    faiss::RangeSearchResult result;
    int nq;
    
    RangeSearchResultWrapper(int nq_) : result(nq_), nq(nq_) {}
};

// ==================== Index Factory ====================

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexFactoryCreate
  (JNIEnv* env, jclass, jint dimension, jstring description, jint metricType) {
    FAISS_TRY
        std::string desc = jstringToString(env, description);
        faiss::MetricType metric = (metricType == 0) ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
        faiss::Index* index = faiss::index_factory(dimension, desc.c_str(), metric);
        return reinterpret_cast<jlong>(index);
    FAISS_CATCH(env)
    return 0;
}

// ==================== Index Operations ====================

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexDestroy
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        delete getIndex(handle);
    FAISS_CATCH(env)
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetDimension
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jint>(getIndex(handle)->d);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetCount
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jlong>(getIndex(handle)->ntotal);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jboolean JNICALL Java_org_apache_paimon_faiss_FaissNative_indexIsTrained
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return getIndex(handle)->is_trained ? JNI_TRUE : JNI_FALSE;
    FAISS_CATCH(env)
    return JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_indexGetMetricType
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        faiss::MetricType metric = getIndex(handle)->metric_type;
        return (metric == faiss::METRIC_L2) ? 0 : 1;
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexTrain
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray vectors) {
    FAISS_TRY
        jfloat* vectorData = env->GetFloatArrayElements(vectors, nullptr);
        getIndex(handle)->train(n, vectorData);
        env->ReleaseFloatArrayElements(vectors, vectorData, JNI_ABORT);
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAdd
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray vectors) {
    FAISS_TRY
        jfloat* vectorData = env->GetFloatArrayElements(vectors, nullptr);
        getIndex(handle)->add(n, vectorData);
        env->ReleaseFloatArrayElements(vectors, vectorData, JNI_ABORT);
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAddWithIds
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray vectors, jlongArray ids) {
    FAISS_TRY
        jfloat* vectorData = env->GetFloatArrayElements(vectors, nullptr);
        jlong* idData = env->GetLongArrayElements(ids, nullptr);
        
        // Convert jlong to faiss::idx_t if needed
        std::vector<faiss::idx_t> faissIds(n);
        for (jlong i = 0; i < n; i++) {
            faissIds[i] = static_cast<faiss::idx_t>(idData[i]);
        }
        
        getIndex(handle)->add_with_ids(n, vectorData, faissIds.data());
        
        env->ReleaseFloatArrayElements(vectors, vectorData, JNI_ABORT);
        env->ReleaseLongArrayElements(ids, idData, JNI_ABORT);
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSearch
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray queries, jint k,
   jfloatArray distances, jlongArray labels) {
    FAISS_TRY
        jfloat* queryData = env->GetFloatArrayElements(queries, nullptr);
        jfloat* distData = env->GetFloatArrayElements(distances, nullptr);
        jlong* labelData = env->GetLongArrayElements(labels, nullptr);
        
        // Use temporary vectors for faiss
        std::vector<faiss::idx_t> faissLabels(n * k);
        
        getIndex(handle)->search(n, queryData, k, distData, faissLabels.data());
        
        // Copy labels back
        for (jlong i = 0; i < n * k; i++) {
            labelData[i] = static_cast<jlong>(faissLabels[i]);
        }
        
        env->ReleaseFloatArrayElements(queries, queryData, JNI_ABORT);
        env->ReleaseFloatArrayElements(distances, distData, 0);
        env->ReleaseLongArrayElements(labels, labelData, 0);
    FAISS_CATCH(env)
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexRangeSearch
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray queries, jfloat radius) {
    FAISS_TRY
        jfloat* queryData = env->GetFloatArrayElements(queries, nullptr);
        
        RangeSearchResultWrapper* wrapper = new RangeSearchResultWrapper(static_cast<int>(n));
        getIndex(handle)->range_search(n, queryData, radius, &wrapper->result);
        
        env->ReleaseFloatArrayElements(queries, queryData, JNI_ABORT);
        return reinterpret_cast<jlong>(wrapper);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexRemoveIds
  (JNIEnv* env, jclass, jlong handle, jlongArray ids) {
    FAISS_TRY
        jsize n = env->GetArrayLength(ids);
        jlong* idData = env->GetLongArrayElements(ids, nullptr);
        
        // Create ID selector
        std::vector<faiss::idx_t> faissIds(n);
        for (jsize i = 0; i < n; i++) {
            faissIds[i] = static_cast<faiss::idx_t>(idData[i]);
        }
        faiss::IDSelectorArray selector(n, faissIds.data());
        
        jlong removed = static_cast<jlong>(getIndex(handle)->remove_ids(selector));
        
        env->ReleaseLongArrayElements(ids, idData, JNI_ABORT);
        return removed;
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexReset
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        getIndex(handle)->reset();
    FAISS_CATCH(env)
}

// ==================== Index I/O ====================

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexWriteToFile
  (JNIEnv* env, jclass, jlong handle, jstring path) {
    FAISS_TRY
        std::string filePath = jstringToString(env, path);
        faiss::write_index(getIndex(handle), filePath.c_str());
    FAISS_CATCH(env)
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexReadFromFile
  (JNIEnv* env, jclass, jstring path) {
    FAISS_TRY
        std::string filePath = jstringToString(env, path);
        faiss::Index* index = faiss::read_index(filePath.c_str());
        return reinterpret_cast<jlong>(index);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSerialize
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        faiss::VectorIOWriter writer;
        faiss::write_index(getIndex(handle), &writer);
        
        jbyteArray result = env->NewByteArray(static_cast<jsize>(writer.data.size()));
        env->SetByteArrayRegion(result, 0, static_cast<jsize>(writer.data.size()),
                                reinterpret_cast<const jbyte*>(writer.data.data()));
        return result;
    FAISS_CATCH(env)
    return nullptr;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexDeserialize
  (JNIEnv* env, jclass, jbyteArray data) {
    FAISS_TRY
        jsize length = env->GetArrayLength(data);
        jbyte* bytes = env->GetByteArrayElements(data, nullptr);
        
        faiss::VectorIOReader reader;
        reader.data.resize(length);
        memcpy(reader.data.data(), bytes, length);
        
        faiss::Index* index = faiss::read_index(&reader);
        
        env->ReleaseByteArrayElements(data, bytes, JNI_ABORT);
        return reinterpret_cast<jlong>(index);
    FAISS_CATCH(env)
    return 0;
}

// ==================== Range Search Result ====================

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultDestroy
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        delete reinterpret_cast<RangeSearchResultWrapper*>(handle);
    FAISS_CATCH(env)
}

JNIEXPORT jlongArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLimits
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        jsize n = wrapper->nq + 1;
        jlongArray result = env->NewLongArray(n);
        
        std::vector<jlong> limits(n);
        for (jsize i = 0; i < n; i++) {
            limits[i] = static_cast<jlong>(wrapper->result.lims[i]);
        }
        env->SetLongArrayRegion(result, 0, n, limits.data());
        return result;
    FAISS_CATCH(env)
    return nullptr;
}

JNIEXPORT jlongArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLabels
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        jsize n = static_cast<jsize>(wrapper->result.lims[wrapper->nq]);
        jlongArray result = env->NewLongArray(n);
        
        std::vector<jlong> labels(n);
        for (jsize i = 0; i < n; i++) {
            labels[i] = static_cast<jlong>(wrapper->result.labels[i]);
        }
        env->SetLongArrayRegion(result, 0, n, labels.data());
        return result;
    FAISS_CATCH(env)
    return nullptr;
}

JNIEXPORT jfloatArray JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetDistances
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        jsize n = static_cast<jsize>(wrapper->result.lims[wrapper->nq]);
        jfloatArray result = env->NewFloatArray(n);
        env->SetFloatArrayRegion(result, 0, n, wrapper->result.distances);
        return result;
    FAISS_CATCH(env)
    return nullptr;
}

// ==================== IVF Index Specific ====================

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfGetNprobe
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jint>(getIndexIVF(handle)->nprobe);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfSetNprobe
  (JNIEnv* env, jclass, jlong handle, jint nprobe) {
    FAISS_TRY
        getIndexIVF(handle)->nprobe = static_cast<size_t>(nprobe);
    FAISS_CATCH(env)
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_ivfGetNlist
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jint>(getIndexIVF(handle)->nlist);
    FAISS_CATCH(env)
    return 0;
}

// ==================== HNSW Index Specific ====================

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswGetEfSearch
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jint>(getIndexHNSW(handle)->hnsw.efSearch);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswSetEfSearch
  (JNIEnv* env, jclass, jlong handle, jint efSearch) {
    FAISS_TRY
        getIndexHNSW(handle)->hnsw.efSearch = static_cast<int>(efSearch);
    FAISS_CATCH(env)
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswGetEfConstruction
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        return static_cast<jint>(getIndexHNSW(handle)->hnsw.efConstruction);
    FAISS_CATCH(env)
    return 0;
}

// ==================== Utility ====================

JNIEXPORT jstring JNICALL Java_org_apache_paimon_faiss_FaissNative_getVersion
  (JNIEnv* env, jclass) {
    // Return FAISS version defined in CMakeLists.txt
    return env->NewStringUTF(FAISS_VERSION);
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_setNumThreads
  (JNIEnv* env, jclass, jint numThreads) {
#ifdef _OPENMP
    omp_set_num_threads(numThreads);
#else
    // OpenMP not available, ignore
    (void)numThreads;
#endif
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_getNumThreads
  (JNIEnv* env, jclass) {
#ifdef _OPENMP
    return omp_get_max_threads();
#else
    return 1;
#endif
}

