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

use jni::objects::GlobalRef;
use jni::JavaVM;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::directory::{Directory, TerminatingWrite, AntiCallToken};
use tantivy::HasLen;

/// File metadata within the archive: offset and length in the stream.
#[derive(Clone, Debug)]
struct FileMeta {
    offset: u64,
    length: u64,
}

/// A read-only Tantivy Directory backed by JNI callbacks to a Java SeekableInputStream.
///
/// The archive is a single stream containing multiple files. Each file's position
/// (offset, length) is known upfront. Reads are dispatched via JNI to the Java side.
#[derive(Clone)]
pub struct JniDirectory {
    jvm: Arc<JavaVM>,
    stream_ref: Arc<GlobalRef>,
    files: Arc<HashMap<PathBuf, FileMeta>>,
    /// meta.json content cached after atomic_write
    atomic_data: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    /// Mutex to protect seek+read atomicity on the shared stream
    stream_lock: Arc<Mutex<()>>,
}

impl JniDirectory {
    pub fn new(
        jvm: JavaVM,
        stream_ref: GlobalRef,
        file_entries: Vec<(String, u64, u64)>,
    ) -> Self {
        let mut files = HashMap::new();
        for (name, offset, length) in file_entries {
            files.insert(
                PathBuf::from(&name),
                FileMeta { offset, length },
            );
        }
        JniDirectory {
            jvm: Arc::new(jvm),
            stream_ref: Arc::new(stream_ref),
            files: Arc::new(files),
            atomic_data: Arc::new(Mutex::new(HashMap::new())),
            stream_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Read bytes from the Java stream at a given absolute position.
    /// The stream_lock ensures seek+read is atomic across concurrent Rust threads.
    fn read_from_stream(&self, position: u64, length: usize) -> io::Result<Vec<u8>> {
        let _guard = self.stream_lock.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Stream lock poisoned: {}", e))
        })?;

        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI attach failed: {}", e)))?;

        // Call seek(long)
        env.call_method(self.stream_ref.as_obj(), "seek", "(J)V", &[jni::objects::JValue::Long(position as i64)])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI seek failed: {}", e)))?;

        // Create byte array and call read(byte[], int, int)
        let buf = env
            .new_byte_array(length as i32)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI new_byte_array failed: {}", e)))?;

        let mut total_read = 0i32;
        while (total_read as usize) < length {
            let remaining = length as i32 - total_read;
            let n = env
                .call_method(
                    self.stream_ref.as_obj(),
                    "read",
                    "([BII)I",
                    &[
                        jni::objects::JValue::Object(&buf),
                        jni::objects::JValue::Int(total_read),
                        jni::objects::JValue::Int(remaining),
                    ],
                )
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI read failed: {}", e)))?
                .i()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI read return type: {}", e)))?;

            if n <= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Unexpected EOF: read {} of {} bytes", total_read, length),
                ));
            }
            total_read += n;
        }

        let mut result = vec![0i8; length];
        env.get_byte_array_region(&buf, 0, &mut result)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("JNI get_byte_array_region: {}", e)))?;

        // Safe: i8 and u8 have the same layout
        let result: Vec<u8> = result.into_iter().map(|b| b as u8).collect();
        Ok(result)
    }
}

impl fmt::Debug for JniDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JniDirectory")
            .field("files", &self.files.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A FileHandle backed by JNI stream reads for a single file within the archive.
#[derive(Clone)]
struct JniFileHandle {
    directory: JniDirectory,
    file_offset: u64,
    file_length: usize,
}

impl fmt::Debug for JniFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JniFileHandle")
            .field("offset", &self.file_offset)
            .field("length", &self.file_length)
            .finish()
    }
}

impl HasLen for JniFileHandle {
    fn len(&self) -> usize {
        self.file_length
    }
}

impl FileHandle for JniFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let start = self.file_offset + range.start as u64;
        let length = range.end - range.start;
        let data = self.directory.read_from_stream(start, length)?;
        Ok(OwnedBytes::new(data))
    }
}

impl Directory for JniDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let meta = self
            .files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(Arc::new(JniFileHandle {
            directory: self.clone(),
            file_offset: meta.offset,
            file_length: meta.length as usize,
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path) || self.atomic_data.lock().unwrap().contains_key(path))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        // Check in-memory atomic data first
        if let Some(data) = self.atomic_data.lock().unwrap().get(path) {
            return Ok(data.clone());
        }
        // Fall back to archive
        let meta = self
            .files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        self.read_from_stream(meta.offset, meta.length as usize)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.atomic_data
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), data.to_vec());
        Ok(())
    }

    // --- Read-only: the following are no-ops or unsupported ---

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(())
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        // Tantivy needs this for lock files; provide a dummy writer
        let buf: Vec<u8> = Vec::new();
        Ok(io::BufWriter::new(Box::new(VecTerminatingWrite(buf))))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        // Read-only: no locking needed
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

/// A dummy writer that implements TerminatingWrite for lock file support.
struct VecTerminatingWrite(Vec<u8>);

impl io::Write for VecTerminatingWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for VecTerminatingWrite {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}
