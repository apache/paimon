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

const HTTP_METHODS = new Set(['get', 'post', 'put', 'delete', 'patch', 'head', 'options', 'trace']);

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function decodePointerSegment(segment) {
  return segment.replace(/~1/g, '/').replace(/~0/g, '~');
}

function validateCommon(fileName) {
  const specPath = path.resolve(__dirname, '..', 'static', fileName);
  const spec = yaml.load(fs.readFileSync(specPath, 'utf8'));

  function checkSpec(condition, message) {
    check(condition, `${fileName}: ${message}`);
  }

  function resolveLocalRef(ref) {
    checkSpec(ref.startsWith('#/'), `Only local OpenAPI references are supported, found: ${ref}`);
    return ref
      .slice(2)
      .split('/')
      .map(decodePointerSegment)
      .reduce((current, segment) => {
        checkSpec(
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
    checkSpec(
      !Object.prototype.hasOwnProperty.call(value, 'nullable'),
      'OpenAPI 3.1 schemas must not use nullable',
    );
    if (typeof value.$ref === 'string') {
      resolveLocalRef(value.$ref);
    }
    Object.values(value).forEach(visit);
  }

  function schema(name) {
    const value = spec.components && spec.components.schemas && spec.components.schemas[name];
    checkSpec(value, `Missing OpenAPI schema: ${name}`);
    return value;
  }

  function requireProperties(schemaName, names) {
    const properties = schema(schemaName).properties || {};
    names.forEach((name) => {
      checkSpec(properties[name], `Schema ${schemaName} is missing property: ${name}`);
    });
    return properties;
  }

  function requireRequiredProperties(schemaName, names) {
    const required = schema(schemaName).required || [];
    names.forEach((name) => {
      checkSpec(required.includes(name), `Schema ${schemaName} must require property: ${name}`);
    });
  }

  function requireTypedIntegerProperties(schemaName, names) {
    const properties = requireProperties(schemaName, names);
    names.forEach((name) => {
      checkSpec(
        properties[name].type === 'integer' && properties[name].format === 'int64',
        `Schema ${schemaName}.${name} must be an int64 integer`,
      );
    });
  }

  function requireSchemaReference(schemaName, composition, referencedSchemaName) {
    const references = schema(schemaName)[composition] || [];
    const expected = `#/components/schemas/${referencedSchemaName}`;
    checkSpec(
      references.some((reference) => reference.$ref === expected),
      `Schema ${schemaName}.${composition} is missing reference: ${expected}`,
    );
  }

  function validatePathParameters(resourcePath, pathItem, operation) {
    const templateNames = Array.from(resourcePath.matchAll(/\{([^}]+)\}/g), (match) => match[1]);
    const parameters = [...(pathItem.parameters || []), ...(operation.parameters || [])];
    const pathParameters = parameters.filter((parameter) => parameter.in === 'path');
    templateNames.forEach((name) => {
      const parameter = pathParameters.find((candidate) => candidate.name === name);
      checkSpec(parameter, `Path ${resourcePath} is missing path parameter: ${name}`);
      checkSpec(
        parameter.required === true,
        `Path parameter ${resourcePath}.${name} must be required`,
      );
    });
    pathParameters.forEach((parameter) => {
      checkSpec(
        templateNames.includes(parameter.name),
        `Path ${resourcePath} declares unused path parameter: ${parameter.name}`,
      );
    });
  }

  checkSpec(spec.openapi === '3.1.1', `Expected OpenAPI 3.1.1, found: ${spec.openapi}`);
  checkSpec(
    spec.paths && spec.components && spec.components.schemas,
    'Incomplete OpenAPI document',
  );
  visit(spec);

  const operations = new Map();
  for (const [resourcePath, pathItem] of Object.entries(spec.paths)) {
    for (const [method, operation] of Object.entries(pathItem)) {
      if (!HTTP_METHODS.has(method)) {
        continue;
      }
      validatePathParameters(resourcePath, pathItem, operation);
      checkSpec(
        operation.operationId,
        `Operation ${method.toUpperCase()} ${resourcePath} has no operationId`,
      );
      checkSpec(
        !operations.has(operation.operationId),
        `Duplicate operationId: ${operation.operationId}`,
      );
      operations.set(operation.operationId, operation);
    }
  }

  function requireOperation(operationId) {
    const operation = operations.get(operationId);
    checkSpec(operation, `Missing operationId: ${operationId}`);
    return operation;
  }

  function requireResponses(operationId, statusCodes) {
    const responses = requireOperation(operationId).responses || {};
    statusCodes.forEach((statusCode) => {
      checkSpec(
        responses[statusCode],
        `Operation ${operationId} is missing response: ${statusCode}`,
      );
    });
  }

  return {
    spec,
    operations,
    schema,
    requireOperation,
    requireProperties,
    requireRequiredProperties,
    requireTypedIntegerProperties,
    requireSchemaReference,
    requireResponses,
    checkSpec,
  };
}

function requireArrayOfIdentifiers(contract, schemaName, propertyName) {
  const properties = contract.requireProperties(schemaName, [propertyName]);
  contract.checkSpec(
    properties[propertyName].type === 'array',
    `Schema ${schemaName}.${propertyName} must be an array`,
  );
  contract.checkSpec(
    properties[propertyName].items &&
      properties[propertyName].items.$ref === '#/components/schemas/Identifier',
    `Schema ${schemaName}.${propertyName} items must reference Identifier`,
  );
}

function requireNullableStringProperty(contract, schemaName, propertyName) {
  const property = contract.requireProperties(schemaName, [propertyName])[propertyName];
  contract.checkSpec(
    Array.isArray(property.type) &&
      property.type.includes('string') &&
      property.type.includes('null'),
    `Schema ${schemaName}.${propertyName} must accept string and null`,
  );
}

function requireExactEnum(contract, schemaName, expectedValues) {
  const actualValues = contract.schema(schemaName).enum || [];
  contract.checkSpec(
    actualValues.length === expectedValues.length &&
      expectedValues.every((value) => actualValues.includes(value)),
    `Schema ${schemaName} must define enum values: ${expectedValues.join(', ')}`,
  );
}

function validateCatalogOpenApi() {
  const contract = validateCommon('rest-catalog-open-api.yaml');
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
  ].forEach(contract.requireOperation);

  contract.requireProperties('ConfigResponse', ['defaults', 'overrides']);
  contract.requireProperties('CreateDatabaseRequest', ['name', 'options']);
  contract.requireProperties('AlterDatabaseRequest', ['removals', 'updates']);
  contract.requireProperties('CreateTableRequest', ['identifier', 'schema']);
  contract.requireProperties('AlterTableRequest', ['changes']);
  contract.requireProperties('Identifier', ['database', 'object']);
  contract.requireProperties('Schema', [
    'fields',
    'partitionKeys',
    'primaryKeys',
    'options',
    'comment',
  ]);
  contract.requireProperties('DataField', ['id', 'name', 'type', 'description', 'defaultValue']);

  contract.requireSchemaReference('DataType', 'oneOf', 'VectorType');
  contract.requireProperties('VectorType', ['type', 'element', 'length']);
  contract.requireSchemaReference('SchemaChange', 'anyOf', 'DropPrimaryKey');
  contract.checkSpec(
    contract.schema('BaseSchemaChange').discriminator.mapping.dropPrimaryKey ===
      '#/components/schemas/DropPrimaryKey',
    'BaseSchemaChange discriminator is missing dropPrimaryKey',
  );
  const dropPrimaryKey = contract.requireProperties('DropPrimaryKey', ['action']);
  contract.checkSpec(
    dropPrimaryKey.action.const === 'dropPrimaryKey',
    'Schema DropPrimaryKey.action must be dropPrimaryKey',
  );
  contract.checkSpec(
    contract.schema('BaseInstant').discriminator.propertyName === 'type',
    'BaseInstant discriminator must use the JSON field type',
  );

  const updateViewComment = contract.requireProperties('UpdateViewComment', ['action', 'comment']);
  contract.checkSpec(
    !updateViewComment.key,
    'Schema UpdateViewComment must use comment instead of key',
  );
  ['UpdateComment', 'UpdateViewComment', 'UpdateFunctionComment'].forEach((schemaName) =>
    requireNullableStringProperty(contract, schemaName, 'comment'),
  );

  const errorResourceTypes =
    contract.requireProperties('ErrorResponse', ['resourceType']).resourceType.enum || [];
  ['FUNCTION', 'DEFINITION'].forEach((resourceType) => {
    contract.checkSpec(
      errorResourceTypes.includes(resourceType),
      `Schema ErrorResponse.resourceType is missing value: ${resourceType}`,
    );
  });

  requireArrayOfIdentifiers(contract, 'ListTablesGloballyResponse', 'tables');
  requireArrayOfIdentifiers(contract, 'ListViewsGloballyResponse', 'views');
  requireArrayOfIdentifiers(contract, 'ListFunctionsGloballyResponse', 'functions');
  contract.requireProperties('ListFunctionsGloballyResponse', ['nextPageToken']);
  const getFunctionProperties = contract.requireProperties('GetFunctionResponse', ['uuid']);
  contract.checkSpec(
    getFunctionProperties.uuid.type === 'string',
    'Schema GetFunctionResponse.uuid must be a string',
  );

  ['GetDatabaseResponse', 'GetTableResponse', 'GetViewResponse', 'GetFunctionResponse'].forEach(
    (schemaName) => contract.requireTypedIntegerProperties(schemaName, ['createdAt', 'updatedAt']),
  );
  return contract.operations.size;
}

function validateManagementOpenApi() {
  const contract = validateCommon('rest-management-open-api.yaml');
  const operationIds = ['listPermissions', 'grantPermission', 'revokePermission'];
  operationIds.forEach(contract.requireOperation);
  operationIds.forEach((operationId) =>
    contract.requireResponses(operationId, ['200', '400', '401', '403', '404', '500']),
  );

  const permissionFields = [
    'resourceType',
    'catalog',
    'database',
    'table',
    'function',
    'view',
    'columns',
    'rowFilter',
    'columnMasking',
    'access',
    'principal',
    'expireTime',
  ];
  contract.requireProperties('Permission', permissionFields);
  contract.requireRequiredProperties('Permission', ['resourceType', 'access', 'principal']);

  const grantProperties = contract.requireProperties('GrantPermissionRequest', permissionFields);
  contract.requireRequiredProperties('GrantPermissionRequest', [
    'resourceType',
    'access',
    'principal',
  ]);
  contract.checkSpec(
    !grantProperties.permission,
    'Schema GrantPermissionRequest must use the flat permission shape',
  );

  const revokeFields = [
    'resourceType',
    'catalog',
    'database',
    'table',
    'function',
    'view',
    'columns',
    'access',
    'principal',
  ];
  const revokeProperties = contract.requireProperties('RevokePermissionRequest', revokeFields);
  contract.requireRequiredProperties('RevokePermissionRequest', [
    'resourceType',
    'access',
    'principal',
  ]);
  ['rowFilter', 'columnMasking', 'expireTime'].forEach((field) =>
    contract.checkSpec(
      !revokeProperties[field],
      `Schema RevokePermissionRequest must omit grant-only field: ${field}`,
    ),
  );

  const listProperties = contract.requireProperties('ListPermissionsResponse', [
    'permissions',
    'nextPageToken',
  ]);
  contract.requireRequiredProperties('ListPermissionsResponse', ['permissions']);
  contract.checkSpec(
    listProperties.permissions.type === 'array' &&
      listProperties.permissions.items.$ref === '#/components/schemas/Permission',
    'Schema ListPermissionsResponse.permissions must be an array of Permission',
  );

  const columnSelection = contract.requireProperties('ColumnSelection', [
    'columnNames',
    'excludedColumnNames',
  ]);
  contract.checkSpec(
    columnSelection.columnNames.uniqueItems === true &&
      columnSelection.excludedColumnNames.uniqueItems === true,
    'Schema ColumnSelection values must be unique',
  );
  contract.checkSpec(
    Array.isArray(contract.schema('ColumnSelection').oneOf) &&
      contract.schema('ColumnSelection').oneOf.length === 2,
    'Schema ColumnSelection must require exactly one selection mode',
  );

  contract.requireProperties('RowFilter', ['expression', 'predicate']);
  contract.requireProperties('ColumnMask', ['expression', 'transform']);
  contract.checkSpec(
    contract.schema('ExpireTime').type === 'string',
    'Schema ExpireTime must use the ISO-8601 string representation',
  );

  requireExactEnum(contract, 'ResourceType', [
    'CATALOG',
    'CATALOG_ALL',
    'DATABASE',
    'DATABASE_ALL',
    'TABLE',
    'FUNCTION',
    'VIEW',
    'COLUMN',
    'ROW_FILTER',
    'COLUMN_MASKING',
  ]);

  const listParameters = contract
    .requireOperation('listPermissions')
    .parameters.filter((parameter) => parameter.in === 'query');
  ['resourceType', 'database', 'table', 'function', 'view', 'principal', 'pageToken', 'maxResults'].forEach(
    (name) =>
      contract.checkSpec(
        listParameters.some((parameter) => parameter.name === name),
        `Operation listPermissions is missing query parameter: ${name}`,
      ),
  );
  contract.checkSpec(
    listParameters.find((parameter) => parameter.name === 'resourceType').required === true,
    'Operation listPermissions must require resourceType',
  );

  contract.checkSpec(
    contract.operations.size === operationIds.length,
    'The initial management contract must stay limited to list, grant, and revoke',
  );
  return contract.operations.size;
}

const catalogOperationCount = validateCatalogOpenApi();
const managementOperationCount = validateManagementOpenApi();
console.log(`Validated REST Catalog OpenAPI contract with ${catalogOperationCount} operations.`);
console.log(
  `Validated REST Management OpenAPI contract with ${managementOperationCount} operations.`,
);
