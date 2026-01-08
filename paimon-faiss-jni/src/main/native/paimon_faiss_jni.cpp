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

// Helper to get direct buffer address with validation
static void* getDirectBufferAddress(JNIEnv* env, jobject buffer, const char* name) {
    void* addr = env->GetDirectBufferAddress(buffer);
    if (addr == nullptr) {
        std::string msg = std::string(name) + " buffer is not a direct buffer or address is unavailable";
        throw std::runtime_error(msg);
    }
    return addr;
}

// Range search result wrapper
struct RangeSearchResultWrapper {
    faiss::RangeSearchResult result;
    int nq;
    
    RangeSearchResultWrapper(int nq_) : result(nq_), nq(nq_) {}
};

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

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexReset
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        getIndex(handle)->reset();
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexTrain
  (JNIEnv* env, jclass, jlong handle, jlong n, jobject vectorBuffer) {
    FAISS_TRY
        float* vectorData = static_cast<float*>(getDirectBufferAddress(env, vectorBuffer, "vector"));
        getIndex(handle)->train(n, vectorData);
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAdd
  (JNIEnv* env, jclass, jlong handle, jlong n, jobject vectorBuffer) {
    FAISS_TRY
        float* vectorData = static_cast<float*>(getDirectBufferAddress(env, vectorBuffer, "vector"));
        getIndex(handle)->add(n, vectorData);
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexAddWithIds
  (JNIEnv* env, jclass, jlong handle, jlong n, jobject vectorBuffer, jobject idBuffer) {
    FAISS_TRY
        float* vectorData = static_cast<float*>(getDirectBufferAddress(env, vectorBuffer, "vector"));
        int64_t* idData = static_cast<int64_t*>(getDirectBufferAddress(env, idBuffer, "id"));
        
        // Convert to faiss::idx_t if size differs
        if (sizeof(faiss::idx_t) == sizeof(int64_t)) {
            getIndex(handle)->add_with_ids(n, vectorData, reinterpret_cast<faiss::idx_t*>(idData));
        } else {
            std::vector<faiss::idx_t> faissIds(n);
            for (jlong i = 0; i < n; i++) {
                faissIds[i] = static_cast<faiss::idx_t>(idData[i]);
            }
            getIndex(handle)->add_with_ids(n, vectorData, faissIds.data());
        }
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSearch
  (JNIEnv* env, jclass, jlong handle, jlong n, jfloatArray queryVectors, jint k,
   jfloatArray distances, jlongArray labels) {
    FAISS_TRY
        // Get query vectors
        jfloat* queryData = env->GetFloatArrayElements(queryVectors, nullptr);
        if (queryData == nullptr) {
            throw std::runtime_error("Failed to get query vectors");
        }
        
        // Allocate temporary arrays for results
        std::vector<float> distTemp(n * k);
        std::vector<faiss::idx_t> labelTemp(n * k);
        
        // Perform search
        getIndex(handle)->search(n, queryData, k, distTemp.data(), labelTemp.data());
        
        // Release query array
        env->ReleaseFloatArrayElements(queryVectors, queryData, JNI_ABORT);
        
        // Copy results to output arrays
        env->SetFloatArrayRegion(distances, 0, n * k, distTemp.data());
        
        // Convert labels to jlong
        std::vector<jlong> jlongLabels(n * k);
        for (jlong i = 0; i < n * k; i++) {
            jlongLabels[i] = static_cast<jlong>(labelTemp[i]);
        }
        env->SetLongArrayRegion(labels, 0, n * k, jlongLabels.data());
    FAISS_CATCH(env)
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexRangeSearch
  (JNIEnv* env, jclass, jlong handle, jlong n, jobject queryBuffer, jfloat radius) {
    FAISS_TRY
        float* queryData = static_cast<float*>(getDirectBufferAddress(env, queryBuffer, "query"));
        
        RangeSearchResultWrapper* wrapper = new RangeSearchResultWrapper(static_cast<int>(n));
        getIndex(handle)->range_search(n, queryData, radius, &wrapper->result);
        
        return reinterpret_cast<jlong>(wrapper);
    FAISS_CATCH(env)
    return 0;
}

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
        faiss::Index* index = faiss::read_index(filePath.c_str(), faiss::IO_FLAG_MMAP);
        return reinterpret_cast<jlong>(index);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSerializeSize
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        faiss::VectorIOWriter writer;
        faiss::write_index(getIndex(handle), &writer);
        return static_cast<jlong>(writer.data.size());
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexSerialize
  (JNIEnv* env, jclass, jlong handle, jobject buffer) {
    FAISS_TRY
        void* bufferData = getDirectBufferAddress(env, buffer, "output");
        
        faiss::VectorIOWriter writer;
        faiss::write_index(getIndex(handle), &writer);
        
        jlong capacity = env->GetDirectBufferCapacity(buffer);
        if (static_cast<size_t>(capacity) < writer.data.size()) {
            throw std::runtime_error("Buffer too small for serialized index");
        }
        
        memcpy(bufferData, writer.data.data(), writer.data.size());
        return static_cast<jlong>(writer.data.size());
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_indexDeserialize
  (JNIEnv* env, jclass, jbyteArray data, jlong length) {
    FAISS_TRY
        jbyte* dataPtr = env->GetByteArrayElements(data, nullptr);
        if (dataPtr == nullptr) {
            throw std::runtime_error("Failed to get byte array elements");
        }
        
        faiss::VectorIOReader reader;
        reader.data.resize(length);
        memcpy(reader.data.data(), dataPtr, length);
        
        env->ReleaseByteArrayElements(data, dataPtr, JNI_ABORT);
        
        faiss::Index* index = faiss::read_index(&reader);
        return reinterpret_cast<jlong>(index);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultDestroy
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        delete reinterpret_cast<RangeSearchResultWrapper*>(handle);
    FAISS_CATCH(env)
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetTotalSize
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        return static_cast<jlong>(wrapper->result.lims[wrapper->nq]);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetNumQueries
  (JNIEnv* env, jclass, jlong handle) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        return static_cast<jint>(wrapper->nq);
    FAISS_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLimits
  (JNIEnv* env, jclass, jlong handle, jobject limitsBuffer) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        int64_t* limitsData = static_cast<int64_t*>(getDirectBufferAddress(env, limitsBuffer, "limits"));
        
        for (int i = 0; i <= wrapper->nq; i++) {
            limitsData[i] = static_cast<int64_t>(wrapper->result.lims[i]);
        }
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetLabels
  (JNIEnv* env, jclass, jlong handle, jobject labelsBuffer) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        int64_t* labelsData = static_cast<int64_t*>(getDirectBufferAddress(env, labelsBuffer, "labels"));
        
        size_t total = wrapper->result.lims[wrapper->nq];
        for (size_t i = 0; i < total; i++) {
            labelsData[i] = static_cast<int64_t>(wrapper->result.labels[i]);
        }
    FAISS_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_rangeSearchResultGetDistances
  (JNIEnv* env, jclass, jlong handle, jobject distancesBuffer) {
    FAISS_TRY
        RangeSearchResultWrapper* wrapper = reinterpret_cast<RangeSearchResultWrapper*>(handle);
        float* distData = static_cast<float*>(getDirectBufferAddress(env, distancesBuffer, "distances"));
        
        size_t total = wrapper->result.lims[wrapper->nq];
        memcpy(distData, wrapper->result.distances, total * sizeof(float));
    FAISS_CATCH(env)
}

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

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_FaissNative_hnswSetEfConstruction
  (JNIEnv* env, jclass, jlong handle, jint efConstruction) {
    FAISS_TRY
        getIndexHNSW(handle)->hnsw.efConstruction = static_cast<int>(efConstruction);
    FAISS_CATCH(env)
}

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
