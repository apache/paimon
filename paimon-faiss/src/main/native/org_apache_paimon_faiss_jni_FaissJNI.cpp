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

#include "org_apache_paimon_faiss_jni_FaissJNI.h"

#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIDMap.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/index_io.h>
#include <faiss/impl/io.h>

#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

// Helper class for writing index to memory buffer
class VectorIOWriter : public faiss::IOWriter {
public:
    std::vector<uint8_t> data;

    size_t operator()(const void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        size_t old_size = data.size();
        data.resize(old_size + bytes);
        memcpy(data.data() + old_size, ptr, bytes);
        return nitems;
    }
};

// Helper class for reading index from memory buffer
class VectorIOReader : public faiss::IOReader {
public:
    const uint8_t* data;
    size_t size;
    size_t pos;

    VectorIOReader(const uint8_t* data, size_t size) : data(data), size(size), pos(0) {}

    size_t operator()(void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        if (pos + bytes > this->size) {
            bytes = this->size - pos;
            nitems = bytes / size;
        }
        memcpy(ptr, data + pos, bytes);
        pos += bytes;
        return nitems;
    }
};

// Convert FAISS metric type
static faiss::MetricType getMetricType(int metric) {
    return metric == 0 ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
}

// Throw Java exception
static void throwJavaException(JNIEnv* env, const char* message) {
    jclass exClass = env->FindClass("java/lang/RuntimeException");
    if (exClass != nullptr) {
        env->ThrowNew(exClass, message);
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_createIndex(
    JNIEnv* env, jclass cls, jint dimension, jstring indexType, jint metric) {
    try {
        const char* typeStr = env->GetStringUTFChars(indexType, nullptr);
        std::string type(typeStr);
        env->ReleaseStringUTFChars(indexType, typeStr);

        faiss::Index* index = nullptr;
        faiss::MetricType metricType = getMetricType(metric);

        if (type == "Flat") {
            index = new faiss::IndexIDMap(new faiss::IndexFlat(dimension, metricType));
        } else {
            throwJavaException(env, ("Unknown index type: " + type).c_str());
            return 0;
        }

        return reinterpret_cast<jlong>(index);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_createHnswIndex(
    JNIEnv* env, jclass cls, jint dimension, jint m, jint efConstruction, jint metric) {
    try {
        faiss::MetricType metricType = getMetricType(metric);
        faiss::IndexHNSWFlat* hnsw = new faiss::IndexHNSWFlat(dimension, m, metricType);
        hnsw->hnsw.efConstruction = efConstruction;
        
        // Wrap with IDMap to support custom IDs
        faiss::IndexIDMap* index = new faiss::IndexIDMap(hnsw);
        return reinterpret_cast<jlong>(index);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_createIvfIndex(
    JNIEnv* env, jclass cls, jint dimension, jint nlist, jint metric) {
    try {
        faiss::MetricType metricType = getMetricType(metric);
        faiss::IndexFlat* quantizer = new faiss::IndexFlat(dimension, metricType);
        faiss::IndexIVFFlat* ivf = new faiss::IndexIVFFlat(quantizer, dimension, nlist, metricType);
        ivf->own_fields = true; // Take ownership of quantizer
        
        // Wrap with IDMap to support custom IDs
        faiss::IndexIDMap* index = new faiss::IndexIDMap(ivf);
        return reinterpret_cast<jlong>(index);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_createIvfPqIndex(
    JNIEnv* env, jclass cls, jint dimension, jint nlist, jint m, jint nbits, jint metric) {
    try {
        faiss::MetricType metricType = getMetricType(metric);
        faiss::IndexFlat* quantizer = new faiss::IndexFlat(dimension, metricType);
        faiss::IndexIVFPQ* ivfpq = new faiss::IndexIVFPQ(quantizer, dimension, nlist, m, nbits, metricType);
        ivfpq->own_fields = true; // Take ownership of quantizer
        
        // Wrap with IDMap to support custom IDs
        faiss::IndexIDMap* index = new faiss::IndexIDMap(ivfpq);
        return reinterpret_cast<jlong>(index);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_addVectors(
    JNIEnv* env, jclass cls, jlong indexPtr, jfloatArray vectors, jint n) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        jfloat* data = env->GetFloatArrayElements(vectors, nullptr);
        
        index->add(n, data);
        
        env->ReleaseFloatArrayElements(vectors, data, JNI_ABORT);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_addVectorsWithIds(
    JNIEnv* env, jclass cls, jlong indexPtr, jfloatArray vectors, jlongArray ids, jint n) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        jfloat* data = env->GetFloatArrayElements(vectors, nullptr);
        jlong* idData = env->GetLongArrayElements(ids, nullptr);
        
        // Convert jlong* to faiss::idx_t*
        std::vector<faiss::idx_t> faissIds(n);
        for (int i = 0; i < n; i++) {
            faissIds[i] = static_cast<faiss::idx_t>(idData[i]);
        }
        
        index->add_with_ids(n, data, faissIds.data());
        
        env->ReleaseFloatArrayElements(vectors, data, JNI_ABORT);
        env->ReleaseLongArrayElements(ids, idData, JNI_ABORT);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_trainIndex(
    JNIEnv* env, jclass cls, jlong indexPtr, jfloatArray vectors, jint n) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        jfloat* data = env->GetFloatArrayElements(vectors, nullptr);
        
        index->train(n, data);
        
        env->ReleaseFloatArrayElements(vectors, data, JNI_ABORT);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT jboolean JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_isTrained(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        return index->is_trained ? JNI_TRUE : JNI_FALSE;
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return JNI_FALSE;
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_search(
    JNIEnv* env, jclass cls, jlong indexPtr, jfloatArray queries, jint nq, jint k, 
    jfloatArray distances, jlongArray labels) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        jfloat* queryData = env->GetFloatArrayElements(queries, nullptr);
        jfloat* distData = env->GetFloatArrayElements(distances, nullptr);
        jlong* labelData = env->GetLongArrayElements(labels, nullptr);
        
        // FAISS uses idx_t for labels
        std::vector<faiss::idx_t> faissLabels(nq * k);
        
        index->search(nq, queryData, k, distData, faissLabels.data());
        
        // Copy labels back
        for (int i = 0; i < nq * k; i++) {
            labelData[i] = static_cast<jlong>(faissLabels[i]);
        }
        
        env->ReleaseFloatArrayElements(queries, queryData, JNI_ABORT);
        env->ReleaseFloatArrayElements(distances, distData, 0);
        env->ReleaseLongArrayElements(labels, labelData, 0);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_rangeSearch(
    JNIEnv* env, jclass cls, jlong indexPtr, jfloatArray queries, jint nq, jfloat radius) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        jfloat* queryData = env->GetFloatArrayElements(queries, nullptr);
        
        faiss::RangeSearchResult result(nq);
        index->range_search(nq, queryData, radius, &result);
        
        env->ReleaseFloatArrayElements(queries, queryData, JNI_ABORT);
        
        // Create result arrays
        jclass objectClass = env->FindClass("java/lang/Object");
        jobjectArray resultArray = env->NewObjectArray(3, objectClass, nullptr);
        
        // lims array
        jlongArray limsArray = env->NewLongArray(nq + 1);
        std::vector<jlong> lims(nq + 1);
        for (size_t i = 0; i <= nq; i++) {
            lims[i] = static_cast<jlong>(result.lims[i]);
        }
        env->SetLongArrayRegion(limsArray, 0, nq + 1, lims.data());
        env->SetObjectArrayElement(resultArray, 0, limsArray);
        
        // distances array
        size_t numResults = result.lims[nq];
        jfloatArray distArray = env->NewFloatArray(numResults);
        env->SetFloatArrayRegion(distArray, 0, numResults, result.distances);
        env->SetObjectArrayElement(resultArray, 1, distArray);
        
        // labels array
        jlongArray labelsArray = env->NewLongArray(numResults);
        std::vector<jlong> labels(numResults);
        for (size_t i = 0; i < numResults; i++) {
            labels[i] = static_cast<jlong>(result.labels[i]);
        }
        env->SetLongArrayRegion(labelsArray, 0, numResults, labels.data());
        env->SetObjectArrayElement(resultArray, 2, labelsArray);
        
        return resultArray;
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_setHnswEfSearch(
    JNIEnv* env, jclass cls, jlong indexPtr, jint efSearch) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        
        // Handle IDMap wrapper
        faiss::IndexIDMap* idMap = dynamic_cast<faiss::IndexIDMap*>(index);
        faiss::Index* baseIndex = idMap ? idMap->index : index;
        
        faiss::IndexHNSW* hnsw = dynamic_cast<faiss::IndexHNSW*>(baseIndex);
        if (hnsw) {
            hnsw->hnsw.efSearch = efSearch;
        }
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_setIvfNprobe(
    JNIEnv* env, jclass cls, jlong indexPtr, jint nprobe) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        
        // Handle IDMap wrapper
        faiss::IndexIDMap* idMap = dynamic_cast<faiss::IndexIDMap*>(index);
        faiss::Index* baseIndex = idMap ? idMap->index : index;
        
        faiss::IndexIVF* ivf = dynamic_cast<faiss::IndexIVF*>(baseIndex);
        if (ivf) {
            ivf->nprobe = nprobe;
        }
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_getIndexSize(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        return static_cast<jlong>(index->ntotal);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_getIndexDimension(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        return static_cast<jint>(index->d);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_writeIndex(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        
        VectorIOWriter writer;
        faiss::write_index(index, &writer);
        
        jbyteArray result = env->NewByteArray(writer.data.size());
        env->SetByteArrayRegion(result, 0, writer.data.size(), 
            reinterpret_cast<const jbyte*>(writer.data.data()));
        
        return result;
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return nullptr;
    }
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_readIndex(
    JNIEnv* env, jclass cls, jbyteArray data) {
    try {
        jsize size = env->GetArrayLength(data);
        jbyte* bytes = env->GetByteArrayElements(data, nullptr);
        
        VectorIOReader reader(reinterpret_cast<const uint8_t*>(bytes), size);
        faiss::Index* index = faiss::read_index(&reader);
        
        env->ReleaseByteArrayElements(data, bytes, JNI_ABORT);
        
        return reinterpret_cast<jlong>(index);
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
        return 0;
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_freeIndex(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        delete index;
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT void JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_resetIndex(
    JNIEnv* env, jclass cls, jlong indexPtr) {
    try {
        faiss::Index* index = reinterpret_cast<faiss::Index*>(indexPtr);
        index->reset();
    } catch (const std::exception& e) {
        throwJavaException(env, e.what());
    }
}

JNIEXPORT jstring JNICALL Java_org_apache_paimon_faiss_jni_FaissJNI_getVersion(
    JNIEnv* env, jclass cls) {
    // FAISS doesn't have a version macro, so we return a placeholder
    return env->NewStringUTF("1.7.4");
}

