# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tantivy full-text global index writer compatible with Java's layout."""

import os
import shutil
import struct
import tempfile
from typing import List, Mapping

from pypaimon.globalindex.index_file_utils import new_global_index_file_name
from pypaimon.globalindex.result_entry import ResultEntry
from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
    TantivyFullTextIndexOptions,
)
from pypaimon.schema.data_types import AtomicType, DataType


_FILE_NAME_PREFIX = "tantivy"


class TantivyFullTextIndexWriter:
    """Writer for one Tantivy full-text global index file.

    Row IDs are local to the manifest row range, matching the Java generic
    global index writer contract. Null text values increase row_count but are
    not added to the Tantivy index.
    """

    def __init__(
        self,
        file_io,
        index_path: str,
        data_type: DataType,
        options: Mapping[str, object],
    ):
        validate_text_type(data_type)
        self.file_name = new_global_index_file_name(_FILE_NAME_PREFIX)
        self._file_io = file_io
        self._index_path = index_path.rstrip("/")
        self._index_options = TantivyFullTextIndexOptions.from_options(options)
        if self._index_options.tokenizer == "jieba":
            raise ValueError(
                "PyPaimon Tantivy full-text index build does not support "
                "tantivy.tokenizer=jieba because tantivy-py does not expose "
                "the Java/Rust tantivy_jieba tokenizer. Build jieba full-text "
                "indexes with Java or SQL."
            )
        self._row_count = 0
        self._temp_index_dir = None
        self._tantivy = None
        self._index = None
        self._writer = None
        self._schema = None
        self._closed = False

    @property
    def index_options(self) -> TantivyFullTextIndexOptions:
        return self._index_options

    def write(self, text, relative_row_id: int) -> None:
        if self._closed:
            raise RuntimeError("TantivyFullTextIndexWriter is already closed.")

        self._row_count += 1
        if text is None:
            return

        text = _materialize_text(text)
        self._ensure_writer()
        self._add_document(int(relative_row_id), text)

    def finish(self) -> List[ResultEntry]:
        if self._closed:
            raise RuntimeError("TantivyFullTextIndexWriter is already closed.")
        self._closed = True

        file_path = self._file_path()
        try:
            if self._row_count == 0:
                return []

            self._ensure_writer()
            self._writer.commit()
            if hasattr(self._writer, "wait_merging_threads"):
                self._writer.wait_merging_threads()

            self._file_io.check_or_mkdirs(self._index_path)
            with self._file_io.new_output_stream(file_path) as output_stream:
                _pack_index_directory(self._temp_index_dir, output_stream)
        except Exception:
            self._file_io.delete_quietly(file_path)
            raise
        finally:
            self._writer = None
            self._index = None
            self._schema = None
            self._delete_temp_dir()

        return [
            ResultEntry(
                self.file_name,
                self._row_count,
                self._index_options.serialize(),
            )
        ]

    def close(self) -> None:
        if not self._closed:
            self._closed = True
        self._writer = None
        self._index = None
        self._schema = None
        self._delete_temp_dir()

    def _file_path(self) -> str:
        return "%s/%s" % (self._index_path, self.file_name)

    def _ensure_writer(self) -> None:
        if self._writer is not None:
            return

        try:
            import tantivy
        except ImportError as e:
            raise ImportError(
                "tantivy is required to build Tantivy full-text indexes. "
                "Install tantivy or pypaimon with the appropriate full-text "
                "search dependencies."
            ) from e

        self._verify_tantivy_writer_api(tantivy)
        self._tantivy = tantivy
        self._temp_index_dir = tempfile.mkdtemp(prefix="tantivy-index-")
        self._schema = _build_schema(tantivy, self._index_options)
        self._index = _create_index(tantivy, self._schema, self._temp_index_dir)
        _register_tokenizer(tantivy, self._index, self._index_options)
        self._writer = self._index.writer()

    def _add_document(self, row_id: int, text: str) -> None:
        document = self._tantivy.Document()
        document.add_unsigned("row_id", row_id)
        document.add_text("text", text)
        self._writer.add_document(document)

    def _delete_temp_dir(self) -> None:
        if self._temp_index_dir is not None:
            shutil.rmtree(self._temp_index_dir, ignore_errors=True)
            self._temp_index_dir = None

    def _verify_tantivy_writer_api(self, tantivy) -> None:
        missing = []
        for name in ("Document", "Index", "SchemaBuilder"):
            if not hasattr(tantivy, name):
                missing.append(name)

        if self._index_options.tokenizer_name() != "default":
            for name in ("TextAnalyzerBuilder", "Tokenizer"):
                if not hasattr(tantivy, name):
                    missing.append(name)
            tokenizer = getattr(tantivy, "Tokenizer", None)
            tokenizer_apis = {
                "default": "simple",
                "ngram": "ngram",
                "simple": "simple",
                "whitespace": "whitespace",
                "raw": "raw",
            }
            tokenizer_api = tokenizer_apis.get(self._index_options.tokenizer)
            if (
                tokenizer_api is not None
                and tokenizer is not None
                and not hasattr(tokenizer, tokenizer_api)
            ):
                missing.append("Tokenizer.%s" % tokenizer_api)

        filter_checks = []
        if self._index_options.tokenizer_name() != "default":
            filter_checks.append(("remove_long", "Filter.remove_long"))
        if self._index_options.lower_case:
            filter_checks.append(("lowercase", "Filter.lowercase"))
        if self._index_options.ascii_folding:
            filter_checks.append(("ascii_fold", "Filter.ascii_fold"))
        if self._index_options.stem:
            filter_checks.append(("stemmer", "Filter.stemmer"))
        if self._index_options.remove_stop_words:
            filter_checks.append(("stopword", "Filter.stopword"))
        if self._index_options.stop_word_list():
            filter_checks.append(("custom_stopword", "Filter.custom_stopword"))
        if self._index_options.tokenizer_name() != "default" and filter_checks:
            filter_ = getattr(tantivy, "Filter", None)
            if filter_ is None:
                missing.append("Filter")
            else:
                for attr, api_name in filter_checks:
                    if not hasattr(filter_, attr):
                        missing.append(api_name)
        if missing:
            raise RuntimeError(
                "PyPaimon Tantivy full-text index build requires a tantivy-py "
                "version with writer support. Missing API(s): %s"
                % ", ".join(missing)
            )


def validate_text_type(data_type: DataType) -> None:
    if not isinstance(data_type, AtomicType):
        raise ValueError(
            "Tantivy full-text index requires string type, but got: %s"
            % data_type
        )

    type_name = data_type.type.upper()
    if (
        type_name == "STRING"
        or type_name.startswith("CHAR")
        or type_name.startswith("VARCHAR")
    ):
        return
    raise ValueError(
        "Tantivy full-text index requires string type, but got: %s"
        % data_type
    )


def _create_index(tantivy, schema, path):
    try:
        return tantivy.Index(schema, path=path)
    except TypeError:
        return tantivy.Index(schema, path)


def _build_schema(tantivy, index_options: TantivyFullTextIndexOptions):
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_unsigned_field(
        "row_id", stored=False, indexed=True, fast=True,
    )
    tokenizer_name = index_options.tokenizer_name()
    field_kwargs = {}
    if not index_options.with_position:
        field_kwargs["index_option"] = "freq"
    if tokenizer_name == "default":
        schema_builder.add_text_field(
            "text", stored=False, **field_kwargs,
        )
    else:
        schema_builder.add_text_field(
            "text", stored=False,
            tokenizer_name=tokenizer_name, **field_kwargs,
        )
    return schema_builder.build()


def _register_tokenizer(tantivy, index, index_options: TantivyFullTextIndexOptions):
    if (
        index_options.tokenizer == "default"
        and index_options.tokenizer_name() == "default"
    ):
        return

    if index_options.tokenizer == "ngram":
        tokenizer = tantivy.Tokenizer.ngram(
            min_gram=index_options.ngram_min_gram,
            max_gram=index_options.ngram_max_gram,
            prefix_only=index_options.ngram_prefix_only)
    elif index_options.tokenizer in ("default", "simple"):
        tokenizer = tantivy.Tokenizer.simple()
    elif index_options.tokenizer == "whitespace":
        tokenizer = tantivy.Tokenizer.whitespace()
    elif index_options.tokenizer == "raw":
        tokenizer = tantivy.Tokenizer.raw()
    else:
        raise ValueError("Unsupported Tantivy tokenizer: %s" % index_options.tokenizer)

    analyzer_builder = tantivy.TextAnalyzerBuilder(tokenizer)
    analyzer_builder = analyzer_builder.filter(
        tantivy.Filter.remove_long(index_options.max_token_length))
    if index_options.lower_case:
        analyzer_builder = analyzer_builder.filter(tantivy.Filter.lowercase())
    if index_options.ascii_folding:
        analyzer_builder = analyzer_builder.filter(tantivy.Filter.ascii_fold())
    if index_options.stem:
        analyzer_builder = analyzer_builder.filter(
            tantivy.Filter.stemmer(index_options.language))
    if index_options.remove_stop_words:
        analyzer_builder = analyzer_builder.filter(
            tantivy.Filter.stopword(index_options.language))
    stop_words = index_options.stop_word_list()
    if stop_words:
        analyzer_builder = analyzer_builder.filter(
            tantivy.Filter.custom_stopword(stop_words))
    index.register_tokenizer(index_options.tokenizer_name(), analyzer_builder.build())


def _materialize_text(value) -> str:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if not isinstance(value, str):
        raise ValueError("Unsupported field type: %s" % type(value).__name__)
    return value


def _pack_index_directory(directory: str, output_stream) -> None:
    files = [
        path for path in sorted(os.listdir(directory))
        if os.path.isfile(os.path.join(directory, path))
    ]
    output_stream.write(struct.pack(">i", len(files)))
    for file_name in files:
        file_path = os.path.join(directory, file_name)
        name_bytes = file_name.encode("utf-8")
        output_stream.write(struct.pack(">i", len(name_bytes)))
        output_stream.write(name_bytes)
        output_stream.write(struct.pack(">q", os.path.getsize(file_path)))
        with open(file_path, "rb") as input_stream:
            while True:
                chunk = input_stream.read(8192)
                if not chunk:
                    break
                output_stream.write(chunk)
