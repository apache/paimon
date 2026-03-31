#!/usr/bin/env python3
"""Pure pypaimon repro: read AK/SK then write AK/SK in one process."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
from pypaimon import CatalogFactory
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repro token-cache pollution with pure pypaimon: "
            "read by read_ak/sk, then write by write_ak/sk."
        )
    )
    parser.add_argument("--uri", required=True)
    parser.add_argument("--warehouse", default="xrobot_data_infra")
    parser.add_argument("--region", default="cn-wulanchabu")
    parser.add_argument("--table-identifier", default="xa_db_test.xa_agent_documents")

    parser.add_argument("--read-ak", required=True)
    parser.add_argument("--read-sk", required=True)
    parser.add_argument("--read-sts", default="")

    parser.add_argument("--write-ak", required=True)
    parser.add_argument("--write-sk", required=True)
    parser.add_argument("--write-sts", default="")

    parser.add_argument("--doc-id", default="pageindex-assets-02RLHF")
    parser.add_argument(
        "--source-uri",
        default=(
            "oss://xrobot-model/ironstudio_dev/xa_agent/documents/pageindex/"
            "xa_agent/docs/pageindex-assets-02RLHF/source/"
            "2071a8cbe48c2f9ffd9a6fea0cb3ff1b19d2e6a0e2fa8e6a84d52ba961a6b2b8.md"
        ),
    )
    parser.add_argument("--index-version", default="assets-v1")
    parser.add_argument("--debug-token-cache", action="store_true")
    parser.add_argument("--debug-credentials", action="store_true")
    return parser.parse_args()


def documents_schema() -> pa.Schema:
    return pa.schema(
        [
            ("tenant_id", pa.string()),
            ("doc_id", pa.string()),
            ("source_uri", pa.string()),
            ("title", pa.string()),
            ("biz_tags_json", pa.string()),
            ("source_sha256", pa.string()),
            ("status", pa.string()),
            ("active_index_version", pa.string()),
            ("error_message", pa.string()),
            ("created_at", pa.string()),
            ("updated_at", pa.string()),
            ("source_upload_id", pa.string()),
            ("source_object_key", pa.string()),
            ("source_artifact_id", pa.string()),
            ("source_table_identifier", pa.string()),
            ("source_uploaded_at", pa.string()),
        ]
    )


def _fingerprint(obj: object) -> str:
    try:
        text = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    except Exception:
        text = str(obj)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _mask(value: object) -> str:
    text = "" if value is None else str(value)
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}***{text[-4:]}"


def _normalize_secret(raw: str) -> str:
    """Remove accidental shell artifacts like spaces/newlines/wrapped quotes."""
    return raw.strip().strip('"').strip("'").strip()


def install_token_debug_hooks() -> None:
    orig_get = RESTTokenFileIO._get_cached_token
    orig_set = RESTTokenFileIO._set_cached_token
    orig_refresh = RESTTokenFileIO.refresh_token

    def wrapped_get(self, identifier_str):
        token = orig_get(self, identifier_str)
        print(f"[token-cache-get] id={identifier_str} hit={token is not None}")
        if token is not None:
            print(
                f"  token_hash={_fingerprint(token.token)} expire={token.expire_at_millis}"
            )
        return token

    def wrapped_set(self, identifier_str, token):
        print(
            f"[token-cache-set] id={identifier_str} token_hash={_fingerprint(token.token)} "
            f"expire={token.expire_at_millis}"
        )
        return orig_set(self, identifier_str, token)

    def wrapped_refresh(self):
        result = orig_refresh(self)
        token_dict = self.token.token
        print(
            f"[token-refresh] id={self.identifier} token_hash={_fingerprint(token_dict)} "
            f"access={_mask(token_dict.get('fs.oss.accessKeyId'))} "
            f"has_sts={bool(token_dict.get('fs.oss.securityToken'))}"
        )
        return result

    RESTTokenFileIO._get_cached_token = wrapped_get
    RESTTokenFileIO._set_cached_token = wrapped_set
    RESTTokenFileIO.refresh_token = wrapped_refresh


def build_catalog(
    *,
    uri: str,
    warehouse: str,
    region: str,
    ak: str,
    sk: str,
    sts: str,
):
    ak = _normalize_secret(ak)
    sk = _normalize_secret(sk)
    sts = _normalize_secret(sts)
    options = {
        "metastore": "rest",
        "uri": uri,
        "warehouse": warehouse,
        "token.provider": "dlf",
        "dlf.region": region,
        "dlf.access-key-id": ak,
        "dlf.access-key-secret": sk,
    }
    if sts:
        options["dlf.security-token"] = sts
    return CatalogFactory.create(options)


def build_payload(args: argparse.Namespace) -> pa.Table:
    now = utc_now_iso()
    row = {
        "tenant_id": "xa_agent",
        "doc_id": args.doc_id,
        "source_uri": args.source_uri,
        "title": "pure-pypaimon-repro",
        "biz_tags_json": '{"project":"PageIndex","source":"pure-pypaimon"}',
        "source_sha256": "2071a8cbe48c2f9ffd9a6fea0cb3ff1b19d2e6a0e2fa8e6a84d52ba961a6b2b8",
        "status": "indexed",
        "active_index_version": args.index_version,
        "error_message": "",
        "created_at": now,
        "updated_at": now,
        "source_upload_id": uuid4().hex,
        "source_object_key": "",
        "source_artifact_id": "",
        "source_table_identifier": args.table_identifier,
        "source_uploaded_at": now,
    }
    return pa.Table.from_pydict(
        {key: [value] for key, value in row.items()},
        schema=documents_schema(),
    )


def write_once(table, payload: pa.Table) -> None:
    builder = table.new_batch_write_builder()
    table_write = builder.new_write()
    table_commit = builder.new_commit()
    try:
        table_write.write_arrow(payload)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
    finally:
        try:
            table_write.close()
        except Exception:
            pass
        try:
            table_commit.close()
        except Exception:
            pass


def run_case_write_only(write_catalog, args: argparse.Namespace) -> None:
    print("\n=== CASE A: write-only (write_ak/sk) ===")
    RESTTokenFileIO._TOKEN_CACHE.clear()
    print("[case-a] cleared token cache")
    table = write_catalog.get_table(args.table_identifier)
    write_once(table, build_payload(args))
    print("[case-a] write succeeded")


def run_case_read_then_write(read_catalog, write_catalog, args: argparse.Namespace) -> None:
    print("\n=== CASE B: read(read_ak/sk) -> write(write_ak/sk) ===")
    RESTTokenFileIO._TOKEN_CACHE.clear()
    print("[case-b] cleared token cache")

    read_table = read_catalog.get_table(args.table_identifier)
    reader = read_table.new_read_builder().with_projection(["doc_id"]).new_read()
    splits = read_table.new_read_builder().new_scan().plan().splits()
    _ = reader.to_pandas(splits)
    print("[case-b] read finished")

    write_table = write_catalog.get_table(args.table_identifier)
    write_once(write_table, build_payload(args))
    print("[case-b] write succeeded")


def main() -> int:
    args = parse_args()

    # Normalize user inputs to avoid signature mismatch caused by hidden chars.
    args.read_ak = _normalize_secret(args.read_ak)
    args.read_sk = _normalize_secret(args.read_sk)
    args.read_sts = _normalize_secret(args.read_sts)
    args.write_ak = _normalize_secret(args.write_ak)
    args.write_sk = _normalize_secret(args.write_sk)
    args.write_sts = _normalize_secret(args.write_sts)

    if args.debug_token_cache:
        install_token_debug_hooks()
    if args.debug_credentials:
        print(
            "[cred] read  ak/sk",
            _mask(args.read_ak),
            _mask(args.read_sk),
            "len",
            len(args.read_ak),
            len(args.read_sk),
        )
        print(
            "[cred] write ak/sk",
            _mask(args.write_ak),
            _mask(args.write_sk),
            "len",
            len(args.write_ak),
            len(args.write_sk),
        )

    read_catalog = build_catalog(
        uri=args.uri,
        warehouse=args.warehouse,
        region=args.region,
        ak=args.read_ak,
        sk=args.read_sk,
        sts=args.read_sts,
    )
    write_catalog = build_catalog(
        uri=args.uri,
        warehouse=args.warehouse,
        region=args.region,
        ak=args.write_ak,
        sk=args.write_sk,
        sts=args.write_sts,
    )

    run_case_write_only(write_catalog, args)

    try:
        run_case_read_then_write(read_catalog, write_catalog, args)
    except Exception as exc:
        print(f"[case-b] write failed as expected: {type(exc).__name__}: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
