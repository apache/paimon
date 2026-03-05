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

#include "paimon_lumina_jni.h"

#include <lumina/api/LuminaBuilder.h>
#include <lumina/api/LuminaSearcher.h>
#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/core/Constants.h>
#include <lumina/core/Types.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#define LUMINA_TRY try {
#define LUMINA_CATCH(env) \
    } catch (const std::exception& e) { \
        jclass exceptionClass = env->FindClass("org/apache/paimon/lumina/LuminaException"); \
        if (exceptionClass != nullptr) { \
            env->ThrowNew(exceptionClass, e.what()); \
        } \
    } catch (...) { \
        jclass exceptionClass = env->FindClass("org/apache/paimon/lumina/LuminaException"); \
        if (exceptionClass != nullptr) { \
            env->ThrowNew(exceptionClass, "Unknown native exception"); \
        } \
    }

static std::string jstringToString(JNIEnv* env, jstring jstr) {
    if (jstr == nullptr) {
        return "";
    }
    const char* chars = env->GetStringUTFChars(jstr, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(jstr, chars);
    return result;
}

static void* getDirectBufferAddress(JNIEnv* env, jobject buffer, const char* name) {
    void* addr = env->GetDirectBufferAddress(buffer);
    if (addr == nullptr) {
        std::string msg = std::string(name) + " buffer is not a direct buffer or address is unavailable";
        throw std::runtime_error(msg);
    }
    return addr;
}

// Wrapper to hold a heap-allocated LuminaBuilder (pImpl is move-only)
struct BuilderWrapper {
    lumina::api::LuminaBuilder builder;
    BuilderWrapper(lumina::api::LuminaBuilder&& b) : builder(std::move(b)) {}
};

// Wrapper to hold a heap-allocated LuminaSearcher (pImpl is move-only)
struct SearcherWrapper {
    lumina::api::LuminaSearcher searcher;
    SearcherWrapper(lumina::api::LuminaSearcher&& s) : searcher(std::move(s)) {}
};

// Parse JNI string arrays into a Lumina Options object
template <lumina::api::OptionsType T>
static lumina::api::Options<T> parseOptions(JNIEnv* env, jobjectArray keys, jobjectArray values) {
    lumina::api::Options<T> opts;
    if (keys == nullptr || values == nullptr) {
        return opts;
    }
    jsize len = env->GetArrayLength(keys);
    for (jsize i = 0; i < len; i++) {
        jstring jkey = (jstring)env->GetObjectArrayElement(keys, i);
        jstring jval = (jstring)env->GetObjectArrayElement(values, i);
        std::string key = jstringToString(env, jkey);
        std::string val = jstringToString(env, jval);

        // Try parsing as int, then double, then bool, else keep as string
        try {
            size_t pos;
            long long intVal = std::stoll(val, &pos);
            if (pos == val.size()) {
                opts.Set(key, static_cast<int64_t>(intVal));
                continue;
            }
        } catch (...) {}
        try {
            size_t pos;
            double dblVal = std::stod(val, &pos);
            if (pos == val.size()) {
                opts.Set(key, dblVal);
                continue;
            }
        } catch (...) {}
        if (val == "true") {
            opts.Set(key, true);
            continue;
        }
        if (val == "false") {
            opts.Set(key, false);
            continue;
        }
        opts.Set(key, val);
    }
    return opts;
}

// ==================== Builder API ====================

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderCreate
  (JNIEnv* env, jclass, jobjectArray optionKeys, jobjectArray optionValues) {
    LUMINA_TRY
        auto opts = parseOptions<lumina::api::OptionsType::Builder>(env, optionKeys, optionValues);
        auto result = lumina::api::LuminaBuilder::Create(opts);
        if (!result.IsOk()) {
            throw std::runtime_error(
                "Failed to create LuminaBuilder: " + result.GetStatus().Message());
        }
        auto* wrapper = new BuilderWrapper(std::move(result).TakeValue());
        return reinterpret_cast<jlong>(wrapper);
    LUMINA_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderPretrain
  (JNIEnv* env, jclass, jlong handle, jobject vectorBuffer, jlong n) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<BuilderWrapper*>(handle);
        float* data = static_cast<float*>(getDirectBufferAddress(env, vectorBuffer, "vector"));
        auto status = wrapper->builder.Pretrain(data, static_cast<uint64_t>(n));
        if (!status.IsOk()) {
            throw std::runtime_error("Pretrain failed: " + status.Message());
        }
    LUMINA_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderInsertBatch
  (JNIEnv* env, jclass, jlong handle, jobject vectorBuffer, jobject idBuffer, jlong n) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<BuilderWrapper*>(handle);
        float* vectorData = static_cast<float*>(getDirectBufferAddress(env, vectorBuffer, "vector"));
        uint64_t* idData = static_cast<uint64_t*>(getDirectBufferAddress(env, idBuffer, "id"));

        // Java long (int64_t) maps directly to lumina::core::vector_id_t (uint64_t)
        auto status = wrapper->builder.InsertBatch(
            vectorData,
            reinterpret_cast<const lumina::core::vector_id_t*>(idData),
            static_cast<uint64_t>(n));
        if (!status.IsOk()) {
            throw std::runtime_error("InsertBatch failed: " + status.Message());
        }
    LUMINA_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderDump
  (JNIEnv* env, jclass, jlong handle, jstring path) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<BuilderWrapper*>(handle);
        std::string filePath = jstringToString(env, path);

        lumina::api::IOOptions ioOpts;
        ioOpts.Set(std::string(lumina::core::kIndexPath), filePath);
        auto status = wrapper->builder.Dump(ioOpts);
        if (!status.IsOk()) {
            throw std::runtime_error("Dump failed: " + status.Message());
        }
    LUMINA_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_builderDestroy
  (JNIEnv* env, jclass, jlong handle) {
    LUMINA_TRY
        delete reinterpret_cast<BuilderWrapper*>(handle);
    LUMINA_CATCH(env)
}

// ==================== Searcher API ====================

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherCreate
  (JNIEnv* env, jclass, jobjectArray optionKeys, jobjectArray optionValues) {
    LUMINA_TRY
        auto opts = parseOptions<lumina::api::OptionsType::Searcher>(env, optionKeys, optionValues);
        auto result = lumina::api::LuminaSearcher::Create(opts);
        if (!result.IsOk()) {
            throw std::runtime_error(
                "Failed to create LuminaSearcher: " + result.GetStatus().Message());
        }
        auto* wrapper = new SearcherWrapper(std::move(result).TakeValue());
        return reinterpret_cast<jlong>(wrapper);
    LUMINA_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherOpen
  (JNIEnv* env, jclass, jlong handle, jstring path) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<SearcherWrapper*>(handle);
        std::string filePath = jstringToString(env, path);

        lumina::api::IOOptions ioOpts;
        ioOpts.Set(std::string(lumina::core::kIndexPath), filePath);
        auto status = wrapper->searcher.Open(ioOpts);
        if (!status.IsOk()) {
            throw std::runtime_error("Open failed: " + status.Message());
        }
    LUMINA_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherSearch
  (JNIEnv* env, jclass, jlong handle, jfloatArray queryVectors, jint n, jint topk,
   jfloatArray distances, jlongArray ids,
   jobjectArray optionKeys, jobjectArray optionValues) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<SearcherWrapper*>(handle);
        lumina::api::Options<lumina::api::OptionsType::Search> searchOpts;
        searchOpts.Set(std::string(lumina::core::kTopK), static_cast<int64_t>(topk));
        if (optionKeys != nullptr && optionValues != nullptr) {
            jsize optLen = env->GetArrayLength(optionKeys);
            for (jsize i = 0; i < optLen; i++) {
                jstring jkey = (jstring)env->GetObjectArrayElement(optionKeys, i);
                std::string key = jstringToString(env, jkey);
                if (key == "list_size") {
                    jstring jval = (jstring)env->GetObjectArrayElement(optionValues, i);
                    std::string val = jstringToString(env, jval);
                    try {
                        size_t pos;
                        long long intVal = std::stoll(val, &pos);
                        if (pos == val.size()) {
                            searchOpts.Set(std::string("diskann.search.list_size"), static_cast<int64_t>(intVal));
                        }
                    } catch (...) {}
                    break;
                }
            }
        }

        auto info = wrapper->searcher.GetMeta();

        jfloat* queryData = env->GetFloatArrayElements(queryVectors, nullptr);
        if (queryData == nullptr) {
            throw std::runtime_error("Failed to get query vectors");
        }

        std::vector<float> distTemp(static_cast<size_t>(n) * topk);
        std::vector<jlong> idTemp(static_cast<size_t>(n) * topk);

        for (jint qi = 0; qi < n; qi++) {
            lumina::api::Query query(queryData + qi * info.dim, info.dim);
            auto result = wrapper->searcher.Search(query, searchOpts);
            if (!result.IsOk()) {
                env->ReleaseFloatArrayElements(queryVectors, queryData, JNI_ABORT);
                throw std::runtime_error("Search failed: " + result.GetStatus().Message());
            }

            auto& sr = result.Value();
            int hitCount = std::min(static_cast<int>(sr.topk.size()), topk);
            for (int i = 0; i < hitCount; i++) {
                distTemp[qi * topk + i] = sr.topk[i].distance;
                idTemp[qi * topk + i] = static_cast<jlong>(sr.topk[i].id);
            }
            // Fill remaining slots with invalid markers
            for (int i = hitCount; i < topk; i++) {
                distTemp[qi * topk + i] = std::numeric_limits<float>::max();
                idTemp[qi * topk + i] = -1;
            }
        }

        env->ReleaseFloatArrayElements(queryVectors, queryData, JNI_ABORT);
        env->SetFloatArrayRegion(distances, 0, n * topk, distTemp.data());
        env->SetLongArrayRegion(ids, 0, n * topk, idTemp.data());
    LUMINA_CATCH(env)
}

JNIEXPORT jlong JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherGetCount
  (JNIEnv* env, jclass, jlong handle) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<SearcherWrapper*>(handle);
        auto info = wrapper->searcher.GetMeta();
        return static_cast<jlong>(info.count);
    LUMINA_CATCH(env)
    return 0;
}

JNIEXPORT jint JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherGetDimension
  (JNIEnv* env, jclass, jlong handle) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<SearcherWrapper*>(handle);
        auto info = wrapper->searcher.GetMeta();
        return static_cast<jint>(info.dim);
    LUMINA_CATCH(env)
    return 0;
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherClose
  (JNIEnv* env, jclass, jlong handle) {
    LUMINA_TRY
        auto* wrapper = reinterpret_cast<SearcherWrapper*>(handle);
        auto status = wrapper->searcher.Close();
        if (!status.IsOk()) {
            throw std::runtime_error("Close failed: " + status.Message());
        }
    LUMINA_CATCH(env)
}

JNIEXPORT void JNICALL Java_org_apache_paimon_lumina_LuminaNative_searcherDestroy
  (JNIEnv* env, jclass, jlong handle) {
    LUMINA_TRY
        delete reinterpret_cast<SearcherWrapper*>(handle);
    LUMINA_CATCH(env)
}
