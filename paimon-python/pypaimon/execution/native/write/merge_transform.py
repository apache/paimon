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

"""Transport joined Arrow batches to the Core MERGE action executor."""


def _callable_assignment(function, on_pairs, callable_input_columns=None, hidden_columns=()):
    from pypaimon.write.merge_transform import _merge_callable_input

    def evaluate(batch):
        if hidden_columns:
            batch = batch.drop(hidden_columns)
        if callable_input_columns is not None:
            import pyarrow as pa
            view = pa.Table.from_arrays(
                [batch.column('t.' + name) for name in callable_input_columns],
                names=list(callable_input_columns),
            )
        else:
            view = _merge_callable_input(batch, on_pairs)
        return function(view)

    return evaluate


def encode_merge_clause(clause, on_pairs=(), on_map=None, input_schema=None,
                        callable_input_columns=None, hidden_columns=(), not_matched=False):
    """Adapt a normalized PyPaimon clause to the typed SDK without running callbacks."""
    from pypaimon_rust.datafusion import MergeAssignment, WhenMatched, WhenNotMatched
    from pypaimon.write.merge_types import SourceColumnRef, TargetColumnRef, LiteralValue
    from pypaimon.write.merge_condition import remap_source_on_keys, rewrite_condition

    available = None if input_schema is None else set(input_schema.names)
    values = {}
    for name, value in clause.spec.items():
        if isinstance(value, SourceColumnRef):
            kind, column = 'source', value.column
            if available is not None and 's.' + column not in available:
                target = next((target for source, target in on_pairs
                               if source == column and 't.' + target in available), None)
                if target is None:
                    values[name] = MergeAssignment.literal(None)
                    continue
                kind, column = 'target', target
            values[name] = (MergeAssignment.source_column(column) if kind == 'source' else
                            MergeAssignment.target_column(column))
        elif isinstance(value, TargetColumnRef):
            if available is not None and 't.' + value.column not in available:
                values[name] = MergeAssignment.literal(None)
            else:
                values[name] = MergeAssignment.target_column(value.column)
        elif isinstance(value, LiteralValue):
            values[name] = MergeAssignment.literal(value.value)
        elif callable(value) and not isinstance(value, type):
            values[name] = MergeAssignment.function(_callable_assignment(
                value, on_pairs, callable_input_columns, hidden_columns))
        else:
            return None
    condition = None
    if clause.condition is not None:
        rewritten = rewrite_condition(clause.condition)
        if on_map:
            rewritten = remap_source_on_keys(rewritten, on_map)
        condition = rewritten
    if not_matched:
        return WhenNotMatched(values, condition)
    if clause.delete:
        return WhenMatched.delete(condition)
    return WhenMatched.update(values, condition)


class _NativeMergeTransform:
    def __init__(self, clauses, output_schema, mode, on_pairs=(), on_map=None,
                 callable_input_columns=None, row_id_name='_ROW_ID'):
        # Require this release's protocol before consuming input. An installed
        # incompatible SDK must not silently select another executor.
        from pypaimon_rust.datafusion import (  # noqa: F401
            MergeAssignment, MergeBatchExecution, WhenMatched, WhenNotMatched,
        )

        self.clauses = clauses
        self.output_schema = output_schema
        self.mode = mode
        self.on_pairs = on_pairs
        self.on_map = on_map
        self.callable_input_columns = callable_input_columns
        self.row_id_name = row_id_name
        self._execution = None

    def __getstate__(self):
        # Ray transports the request; each worker compiles its own operator.
        state = self.__dict__.copy()
        state['_execution'] = None
        return state

    def __call__(self, batch):
        if batch.num_rows == 0:
            return self.output_schema.empty_table()
        output_schema = self.output_schema
        hidden_columns = ()
        if self.mode != 'insert' and self.row_id_name != '_ROW_ID':
            batch = batch.append_column('t._ROW_ID', batch.column('t.' + self.row_id_name))
            hidden_columns = ('t._ROW_ID',)
            index = output_schema.get_field_index(self.row_id_name)
            output_schema = output_schema.set(index, output_schema.field(index).with_name('_ROW_ID'))
        if self._execution is None:
            from pypaimon_rust.datafusion import MergeBatchExecution

            clauses = [encode_merge_clause(
                clause, self.on_pairs, self.on_map, batch.schema,
                self.callable_input_columns, hidden_columns, not_matched=self.mode == 'insert',
            ) for clause in self.clauses]
            if any(clause is None for clause in clauses):
                raise TypeError('Unsupported normalized MERGE assignment')
            if self.mode == 'update':
                self._execution = MergeBatchExecution.for_updates(batch.schema, output_schema, clauses)
            elif self.mode == 'delete':
                self._execution = MergeBatchExecution.for_deletes(batch.schema, clauses)
            else:
                self._execution = MergeBatchExecution.for_inserts(batch.schema, output_schema, clauses)
        result = self._execution.execute(batch)
        if hidden_columns or (self.mode == 'delete' and not result.schema.equals(
                self.output_schema, check_metadata=True)):
            import pyarrow as pa
            result = pa.Table.from_arrays(result.columns, schema=self.output_schema)
        return result


def _build_matched_transform(
    clauses, on_map, on_pairs, update_cols, row_id_name,
    update_schema, callable_input_columns=None,
):
    return _NativeMergeTransform(
        clauses, update_schema, 'update', on_pairs, on_map,
        callable_input_columns, row_id_name,
    )


def _build_matched_delete_transform(clauses, on_map, row_id_name, delete_schema):
    return _NativeMergeTransform(
        clauses, delete_schema, 'delete', on_map=on_map, row_id_name=row_id_name,
    )


def _build_insert_transform(clauses, target_field_names, target_pa_schema):
    return _NativeMergeTransform(clauses, target_pa_schema, 'insert')
