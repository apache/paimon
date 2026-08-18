/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const specPath = path.resolve(__dirname, '..', 'static', 'rest-catalog-open-api.yaml');
const spec = yaml.load(fs.readFileSync(specPath, 'utf8'));

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function decodePointerSegment(segment) {
  return segment.replace(/~1/g, '/').replace(/~0/g, '~');
}

function resolveLocalRef(ref) {
  check(ref.startsWith('#/'), `Only local OpenAPI references are supported, found: ${ref}`);
  return ref
    .slice(2)
    .split('/')
    .map(decodePointerSegment)
    .reduce((current, segment) => {
      check(
        current && Object.prototype.hasOwnProperty.call(current, segment),
        `Unresolved OpenAPI reference: ${ref}`,
      );
      return current[segment];
    }, spec);
}

function visit(value) {
  if (Array.isArray(value)) {
    value.forEach(visit);
    return;
  }
  if (!value || typeof value !== 'object') {
    return;
  }
  check(!Object.prototype.hasOwnProperty.call(value, 'nullable'), 'OpenAPI 3.1 schemas must not use nullable');
  if (typeof value.$ref === 'string') {
    resolveLocalRef(value.$ref);
  }
  Object.values(value).forEach(visit);
}

function schema(name) {
  const value = spec.components && spec.components.schemas && spec.components.schemas[name];
  check(value, `Missing OpenAPI schema: ${name}`);
  return value;
}

function requireProperties(schemaName, names) {
  const properties = schema(schemaName).properties || {};
  names.forEach((name) => {
    check(properties[name], `Schema ${schemaName} is missing property: ${name}`);
  });
  return properties;
}

function requireTypedIntegerProperties(schemaName, names) {
  const properties = requireProperties(schemaName, names);
  names.forEach((name) => {
    check(
      properties[name].type === 'integer' && properties[name].format === 'int64',
      `Schema ${schemaName}.${name} must be an int64 integer`,
    );
  });
}

function requireSchemaReference(schemaName, composition, referencedSchemaName) {
  const references = schema(schemaName)[composition] || [];
  const expected = `#/components/schemas/${referencedSchemaName}`;
  check(
    references.some((reference) => reference.$ref === expected),
    `Schema ${schemaName}.${composition} is missing reference: ${expected}`,
  );
}

function requireArrayOfIdentifiers(schemaName, propertyName) {
  const properties = requireProperties(schemaName, [propertyName]);
  check(properties[propertyName].type === 'array', `Schema ${schemaName}.${propertyName} must be an array`);
  check(
    properties[propertyName].items &&
      properties[propertyName].items.$ref === '#/components/schemas/Identifier',
    `Schema ${schemaName}.${propertyName} items must reference Identifier`,
  );
}

function requireNullableStringProperty(schemaName, propertyName) {
  const property = requireProperties(schemaName, [propertyName])[propertyName];
  check(
    Array.isArray(property.type) &&
      property.type.includes('string') &&
      property.type.includes('null'),
    `Schema ${schemaName}.${propertyName} must accept string and null`,
  );
}

check(spec.openapi === '3.1.1', `Expected OpenAPI 3.1.1, found: ${spec.openapi}`);
check(spec.paths && spec.components && spec.components.schemas, 'Incomplete OpenAPI document');
visit(spec);

const operationIds = new Set();
for (const pathItem of Object.values(spec.paths)) {
  for (const operation of Object.values(pathItem)) {
    if (!operation || typeof operation !== 'object' || !operation.operationId) {
      continue;
    }
    check(!operationIds.has(operation.operationId), `Duplicate operationId: ${operation.operationId}`);
    operationIds.add(operation.operationId);
  }
}

[
  'getConfig',
  'createDatabase',
  'getDatabase',
  'alterDatabase',
  'dropDatabase',
  'createTable',
  'getTable',
  'alterTable',
  'dropTable',
].forEach((operationId) => {
  check(operationIds.has(operationId), `Missing provider-facing operationId: ${operationId}`);
});

requireProperties('ConfigResponse', ['defaults', 'overrides']);
requireProperties('CreateDatabaseRequest', ['name', 'options']);
requireProperties('AlterDatabaseRequest', ['removals', 'updates']);
requireProperties('CreateTableRequest', ['identifier', 'schema']);
requireProperties('AlterTableRequest', ['changes']);
requireProperties('Identifier', ['database', 'object']);
requireProperties('Schema', ['fields', 'partitionKeys', 'primaryKeys', 'options', 'comment']);
requireProperties('DataField', ['id', 'name', 'type', 'description', 'defaultValue']);

requireSchemaReference('DataType', 'oneOf', 'VectorType');
requireProperties('VectorType', ['type', 'element', 'length']);
requireSchemaReference('SchemaChange', 'anyOf', 'DropPrimaryKey');
check(
  schema('BaseSchemaChange').discriminator.mapping.dropPrimaryKey ===
    '#/components/schemas/DropPrimaryKey',
  'BaseSchemaChange discriminator is missing dropPrimaryKey',
);
const dropPrimaryKey = requireProperties('DropPrimaryKey', ['action']);
check(
  dropPrimaryKey.action.const === 'dropPrimaryKey',
  'Schema DropPrimaryKey.action must be dropPrimaryKey',
);
check(
  schema('BaseInstant').discriminator.propertyName === 'type',
  'BaseInstant discriminator must use the JSON field type',
);

const updateViewComment = requireProperties('UpdateViewComment', ['action', 'comment']);
check(!updateViewComment.key, 'Schema UpdateViewComment must use comment instead of key');
['UpdateComment', 'UpdateViewComment', 'UpdateFunctionComment'].forEach((schemaName) =>
  requireNullableStringProperty(schemaName, 'comment'),
);

const errorResourceTypes = requireProperties('ErrorResponse', ['resourceType']).resourceType.enum || [];
['FUNCTION', 'DEFINITION'].forEach((resourceType) => {
  check(
    errorResourceTypes.includes(resourceType),
    `Schema ErrorResponse.resourceType is missing value: ${resourceType}`,
  );
});

requireArrayOfIdentifiers('ListTablesGloballyResponse', 'tables');
requireArrayOfIdentifiers('ListViewsGloballyResponse', 'views');
requireArrayOfIdentifiers('ListFunctionsGloballyResponse', 'functions');
requireProperties('ListFunctionsGloballyResponse', ['nextPageToken']);
const getFunctionProperties = requireProperties('GetFunctionResponse', ['uuid']);
check(getFunctionProperties.uuid.type === 'string', 'Schema GetFunctionResponse.uuid must be a string');

['GetDatabaseResponse', 'GetTableResponse', 'GetViewResponse', 'GetFunctionResponse'].forEach(
  (schemaName) => requireTypedIntegerProperties(schemaName, ['createdAt', 'updatedAt']),
);

console.log(`Validated REST OpenAPI contract with ${operationIds.size} operations.`);
