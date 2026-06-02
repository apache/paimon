// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod jni_directory;

use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jfloat, jint, jlong, jobject};
use jni::JNIEnv;
use std::ptr;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};
use tantivy_jieba::JiebaTokenizer;

use crate::jni_directory::JniDirectory;

/// Helper: throw a Java RuntimeException and return a default value.
fn throw_and_return<T: Default>(env: &mut JNIEnv, msg: &str) -> T {
    let _ = env.throw_new("java/lang/RuntimeException", msg);
    T::default()
}

/// Helper: throw a Java RuntimeException and return a null jobject.
fn throw_and_return_null(env: &mut JNIEnv, msg: &str) -> jobject {
    let _ = env.throw_new("java/lang/RuntimeException", msg);
    ptr::null_mut()
}

/// Fixed schema: rowId (u64 fast field) + text (full-text indexed).
struct TantivyIndex {
    writer: IndexWriter,
    row_id_field: Field,
    text_field: Field,
}

struct TantivySearcherHandle {
    reader: IndexReader,
    text_field: Field,
}

#[derive(Clone)]
struct TokenizerConfig {
    name: String,
    min_gram: usize,
    max_gram: usize,
    prefix_only: bool,
    lower_case: bool,
}

impl Default for TokenizerConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            min_gram: 2,
            max_gram: 2,
            prefix_only: false,
            lower_case: true,
        }
    }
}

impl TokenizerConfig {
    fn tokenizer_name(&self) -> &str {
        match self.name.as_str() {
            "ngram" => "paimon_ngram",
            "jieba" => "paimon_jieba",
            _ => &self.name,
        }
    }
}

fn register_tokenizer(index: &Index, config: &TokenizerConfig) -> tantivy::Result<()> {
    if config.name == "ngram" {
        let tokenizer = NgramTokenizer::new(config.min_gram, config.max_gram, config.prefix_only)?;
        if config.lower_case {
            index.tokenizers().register(
                config.tokenizer_name(),
                TextAnalyzer::builder(tokenizer).filter(LowerCaser).build(),
            );
        } else {
            index
                .tokenizers()
                .register(config.tokenizer_name(), tokenizer);
        }
    } else if config.name == "jieba" {
        let tokenizer = JiebaTokenizer {};
        if config.lower_case {
            index.tokenizers().register(
                config.tokenizer_name(),
                TextAnalyzer::builder(tokenizer).filter(LowerCaser).build(),
            );
        } else {
            index
                .tokenizers()
                .register(config.tokenizer_name(), tokenizer);
        }
    }
    Ok(())
}

fn tokenizer_config_from_java(
    env: &mut JNIEnv,
    tokenizer_name: JString,
    min_gram: jint,
    max_gram: jint,
    prefix_only: jboolean,
    lower_case: jboolean,
) -> Result<TokenizerConfig, String> {
    let name: String = env
        .get_string(&tokenizer_name)
        .map_err(|e| format!("Failed to get tokenizer name: {}", e))?
        .into();
    let name = name.trim().to_lowercase();
    if name != "default" && name != "ngram" && name != "jieba" {
        return Err(format!("Unsupported tokenizer: {}", name));
    }
    if min_gram <= 0 {
        return Err(format!("minGram must be positive, got {}", min_gram));
    }
    if max_gram <= 0 {
        return Err(format!("maxGram must be positive, got {}", max_gram));
    }
    if min_gram > max_gram {
        return Err(format!(
            "minGram must not be greater than maxGram, got {} > {}",
            min_gram, max_gram
        ));
    }
    Ok(TokenizerConfig {
        name,
        min_gram: min_gram as usize,
        max_gram: max_gram as usize,
        prefix_only: prefix_only != 0,
        lower_case: lower_case != 0,
    })
}

fn build_schema(config: &TokenizerConfig) -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let row_id_field =
        builder.add_u64_field("row_id", NumericOptions::default().set_indexed().set_fast());
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(config.tokenizer_name())
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let text_field = builder.add_text_field("text", text_options);
    (builder.build(), row_id_field, text_field)
}

fn create_index_internal(mut env: JNIEnv, index_path: JString, config: TokenizerConfig) -> jlong {
    let path: String = match env.get_string(&index_path) {
        Ok(s) => s.into(),
        Err(e) => return throw_and_return(&mut env, &format!("Failed to get index path: {}", e)),
    };
    let (schema, row_id_field, text_field) = build_schema(&config);

    let dir = std::path::Path::new(&path);
    if let Err(e) = std::fs::create_dir_all(dir) {
        return throw_and_return(&mut env, &format!("Failed to create directory: {}", e));
    }
    let index = match Index::create_in_dir(dir, schema) {
        Ok(i) => i,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to create index: {}", e)),
    };
    if let Err(e) = register_tokenizer(&index, &config) {
        return throw_and_return(&mut env, &format!("Failed to register tokenizer: {}", e));
    }
    let writer = match index.writer(50_000_000) {
        Ok(w) => w,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to create writer: {}", e)),
    };

    let handle = Box::new(TantivyIndex {
        writer,
        row_id_field,
        text_field,
    });
    Box::into_raw(handle) as jlong
}

fn open_index_internal(mut env: JNIEnv, index_path: JString, config: TokenizerConfig) -> jlong {
    let path: String = match env.get_string(&index_path) {
        Ok(s) => s.into(),
        Err(e) => return throw_and_return(&mut env, &format!("Failed to get index path: {}", e)),
    };
    let index = match Index::open_in_dir(&path) {
        Ok(i) => i,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to open index: {}", e)),
    };
    if let Err(e) = register_tokenizer(&index, &config) {
        return throw_and_return(&mut env, &format!("Failed to register tokenizer: {}", e));
    }
    let schema = index.schema();

    let text_field = schema.get_field("text").unwrap();

    let reader = match index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
    {
        Ok(r) => r,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to create reader: {}", e)),
    };

    let handle = Box::new(TantivySearcherHandle { reader, text_field });
    Box::into_raw(handle) as jlong
}

fn open_from_stream_internal(
    mut env: JNIEnv,
    file_names: jni::objects::JObjectArray,
    file_offsets: jni::objects::JLongArray,
    file_lengths: jni::objects::JLongArray,
    stream_input: JObject,
    config: TokenizerConfig,
) -> jlong {
    // Parse file metadata from Java arrays
    let count = match env.get_array_length(&file_names) {
        Ok(c) => c as usize,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to get array length: {}", e)),
    };
    let mut offsets_buf = vec![0i64; count];
    let mut lengths_buf = vec![0i64; count];
    if let Err(e) = env.get_long_array_region(&file_offsets, 0, &mut offsets_buf) {
        return throw_and_return(&mut env, &format!("Failed to get offsets: {}", e));
    }
    if let Err(e) = env.get_long_array_region(&file_lengths, 0, &mut lengths_buf) {
        return throw_and_return(&mut env, &format!("Failed to get lengths: {}", e));
    }

    let mut files = Vec::with_capacity(count);
    for i in 0..count {
        let obj = match env.get_object_array_element(&file_names, i as i32) {
            Ok(o) => o,
            Err(e) => {
                return throw_and_return(
                    &mut env,
                    &format!("Failed to get file name at {}: {}", i, e),
                )
            }
        };
        let jstr = JString::from(obj);
        let name: String = match env.get_string(&jstr) {
            Ok(s) => s.into(),
            Err(e) => {
                return throw_and_return(&mut env, &format!("Failed to convert file name: {}", e))
            }
        };
        files.push((name, offsets_buf[i] as u64, lengths_buf[i] as u64));
    }

    // Create a global ref to the Java stream callback
    let jvm = match env.get_java_vm() {
        Ok(v) => v,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to get JVM: {}", e)),
    };
    let stream_ref = match env.new_global_ref(stream_input) {
        Ok(r) => r,
        Err(e) => {
            return throw_and_return(&mut env, &format!("Failed to create global ref: {}", e))
        }
    };

    let directory = JniDirectory::new(jvm, stream_ref, files);
    let index = match Index::open(directory) {
        Ok(i) => i,
        Err(e) => {
            return throw_and_return(
                &mut env,
                &format!("Failed to open index from stream: {}", e),
            )
        }
    };
    if let Err(e) = register_tokenizer(&index, &config) {
        return throw_and_return(&mut env, &format!("Failed to register tokenizer: {}", e));
    }
    let schema = index.schema();

    let text_field = schema.get_field("text").unwrap();

    let reader = match index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
    {
        Ok(r) => r,
        Err(e) => return throw_and_return(&mut env, &format!("Failed to create reader: {}", e)),
    };

    let handle = Box::new(TantivySearcherHandle { reader, text_field });
    Box::into_raw(handle) as jlong
}

// ---------------------------------------------------------------------------
// TantivyIndexWriter native methods
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_createIndex(
    env: JNIEnv,
    _class: JClass,
    index_path: JString,
) -> jlong {
    create_index_internal(env, index_path, TokenizerConfig::default())
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_createIndexWithTokenizer(
    mut env: JNIEnv,
    _class: JClass,
    index_path: JString,
    tokenizer_name: JString,
    min_gram: jint,
    max_gram: jint,
    prefix_only: jboolean,
    lower_case: jboolean,
) -> jlong {
    let config = match tokenizer_config_from_java(
        &mut env,
        tokenizer_name,
        min_gram,
        max_gram,
        prefix_only,
        lower_case,
    ) {
        Ok(config) => config,
        Err(e) => return throw_and_return(&mut env, &e),
    };
    create_index_internal(env, index_path, config)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_writeDocument(
    mut env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
    row_id: jlong,
    text: JString,
) {
    let handle = unsafe { &mut *(index_ptr as *mut TantivyIndex) };
    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            throw_and_return::<()>(&mut env, &format!("Failed to get text string: {}", e));
            return;
        }
    };

    let mut doc = tantivy::TantivyDocument::new();
    doc.add_u64(handle.row_id_field, row_id as u64);
    doc.add_text(handle.text_field, &text_str);
    if let Err(e) = handle.writer.add_document(doc) {
        throw_and_return::<()>(&mut env, &format!("Failed to add document: {}", e));
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_commitIndex(
    mut env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
) {
    let handle = unsafe { &mut *(index_ptr as *mut TantivyIndex) };
    if let Err(e) = handle.writer.commit() {
        throw_and_return::<()>(&mut env, &format!("Failed to commit index: {}", e));
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_freeIndex(
    _env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
) {
    unsafe {
        let _ = Box::from_raw(index_ptr as *mut TantivyIndex);
    }
}

// ---------------------------------------------------------------------------
// TantivySearcher native methods
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openIndex(
    env: JNIEnv,
    _class: JClass,
    index_path: JString,
) -> jlong {
    open_index_internal(env, index_path, TokenizerConfig::default())
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openIndexWithTokenizer(
    mut env: JNIEnv,
    _class: JClass,
    index_path: JString,
    tokenizer_name: JString,
    min_gram: jint,
    max_gram: jint,
    prefix_only: jboolean,
    lower_case: jboolean,
) -> jlong {
    let config = match tokenizer_config_from_java(
        &mut env,
        tokenizer_name,
        min_gram,
        max_gram,
        prefix_only,
        lower_case,
    ) {
        Ok(config) => config,
        Err(e) => return throw_and_return(&mut env, &e),
    };
    open_index_internal(env, index_path, config)
}

/// Open an index from a Java StreamFileInput callback object.
///
/// fileNames: String[] — names of files in the archive
/// fileOffsets: long[] — byte offset of each file in the stream
/// fileLengths: long[] — byte length of each file
/// streamInput: StreamFileInput — Java object with seek(long) and read(byte[], int, int) methods
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openFromStream(
    env: JNIEnv,
    _class: JClass,
    file_names: jni::objects::JObjectArray,
    file_offsets: jni::objects::JLongArray,
    file_lengths: jni::objects::JLongArray,
    stream_input: JObject,
) -> jlong {
    open_from_stream_internal(
        env,
        file_names,
        file_offsets,
        file_lengths,
        stream_input,
        TokenizerConfig::default(),
    )
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openFromStreamWithTokenizer(
    mut env: JNIEnv,
    _class: JClass,
    file_names: jni::objects::JObjectArray,
    file_offsets: jni::objects::JLongArray,
    file_lengths: jni::objects::JLongArray,
    stream_input: JObject,
    tokenizer_name: JString,
    min_gram: jint,
    max_gram: jint,
    prefix_only: jboolean,
    lower_case: jboolean,
) -> jlong {
    let config = match tokenizer_config_from_java(
        &mut env,
        tokenizer_name,
        min_gram,
        max_gram,
        prefix_only,
        lower_case,
    ) {
        Ok(config) => config,
        Err(e) => return throw_and_return(&mut env, &e),
    };
    open_from_stream_internal(
        env,
        file_names,
        file_offsets,
        file_lengths,
        stream_input,
        config,
    )
}

/// Search and return a SearchResult(long[] rowIds, float[] scores).
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_searchIndex(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_string: JString,
    limit: jint,
) -> jobject {
    let handle = unsafe { &*(searcher_ptr as *const TantivySearcherHandle) };
    let query_str: String = match env.get_string(&query_string) {
        Ok(s) => s.into(),
        Err(e) => {
            return throw_and_return_null(&mut env, &format!("Failed to get query string: {}", e))
        }
    };

    let searcher = handle.reader.searcher();
    let query_parser = QueryParser::for_index(&searcher.index(), vec![handle.text_field]);
    let query = match query_parser.parse_query(&query_str) {
        Ok(q) => q,
        Err(e) => {
            return throw_and_return_null(
                &mut env,
                &format!("Failed to parse query '{}': {}", query_str, e),
            )
        }
    };
    let top_docs = match searcher.search(&query, &TopDocs::with_limit(limit as usize)) {
        Ok(d) => d,
        Err(e) => return throw_and_return_null(&mut env, &format!("Search failed: {}", e)),
    };

    let count = top_docs.len();

    // Build Java long[] and float[]
    let row_id_array = match env.new_long_array(count as i32) {
        Ok(a) => a,
        Err(e) => {
            return throw_and_return_null(&mut env, &format!("Failed to create long array: {}", e))
        }
    };
    let score_array = match env.new_float_array(count as i32) {
        Ok(a) => a,
        Err(e) => {
            return throw_and_return_null(&mut env, &format!("Failed to create float array: {}", e))
        }
    };

    let mut row_ids: Vec<jlong> = Vec::with_capacity(count);
    let mut scores: Vec<jfloat> = Vec::with_capacity(count);

    // Use fast field reader for efficient row_id retrieval
    for (score, doc_address) in &top_docs {
        let segment_reader = searcher.segment_reader(doc_address.segment_ord);
        let fast_fields = match segment_reader.fast_fields().u64("row_id") {
            Ok(f) => f,
            Err(e) => {
                return throw_and_return_null(&mut env, &format!("Failed to get fast field: {}", e))
            }
        };
        let row_id = fast_fields.first(doc_address.doc_id).unwrap_or(0) as jlong;
        row_ids.push(row_id);
        scores.push(*score as jfloat);
    }

    if let Err(e) = env.set_long_array_region(&row_id_array, 0, &row_ids) {
        return throw_and_return_null(&mut env, &format!("Failed to set long array: {}", e));
    }
    if let Err(e) = env.set_float_array_region(&score_array, 0, &scores) {
        return throw_and_return_null(&mut env, &format!("Failed to set float array: {}", e));
    }

    // Construct SearchResult object
    let class = match env.find_class("org/apache/paimon/tantivy/SearchResult") {
        Ok(c) => c,
        Err(e) => {
            return throw_and_return_null(
                &mut env,
                &format!("Failed to find SearchResult class: {}", e),
            )
        }
    };
    let obj = match env.new_object(
        class,
        "([J[F)V",
        &[
            JValue::Object(&JObject::from(row_id_array)),
            JValue::Object(&JObject::from(score_array)),
        ],
    ) {
        Ok(o) => o,
        Err(e) => {
            return throw_and_return_null(
                &mut env,
                &format!("Failed to create SearchResult: {}", e),
            )
        }
    };

    obj.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_freeSearcher(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    unsafe {
        let _ = Box::from_raw(searcher_ptr as *mut TantivySearcherHandle);
    }
}
