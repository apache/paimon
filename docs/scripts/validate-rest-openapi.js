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
    const parameters = [...(pathItem.parameters || []), ...(operation.parameters || [])].map(
      (parameter) => (parameter.$ref ? resolveLocalRef(parameter.$ref) : parameter),
    );
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
  const operationIds = [
    'listPermissions',
    'grantPermission',
    'revokePermission',
    'listTablePolicies',
    'createTablePolicy',
    'getTablePolicy',
    'createOrReplaceTablePolicy',
    'dropTablePolicy',
  ];
  const resourcePaths = [
    '/v1/{prefix}/permissions',
    '/v1/{prefix}/permissions/grant',
    '/v1/{prefix}/permissions/revoke',
    '/v1/{prefix}/databases/{database}/tables/{table}/policies',
    '/v1/{prefix}/databases/{database}/tables/{table}/policies/{policyName}',
  ];

  resourcePaths.forEach((resourcePath) =>
    contract.checkSpec(
      contract.spec.paths[resourcePath],
      `Missing management path: ${resourcePath}`,
    ),
  );
  [
    '/v1/{prefix}/policies',
    '/v1/{prefix}/databases/{database}/policies',
  ].forEach((resourcePath) =>
    contract.checkSpec(
      !contract.spec.paths[resourcePath],
      `Policies must not be attachable outside tables: ${resourcePath}`,
    ),
  );
  operationIds.forEach(contract.requireOperation);
  ['listPermissions', 'grantPermission', 'revokePermission'].forEach((operationId) =>
    contract.requireResponses(operationId, ['200', '400', '401', '403', '404', '500']),
  );
  ['listTablePolicies', 'createTablePolicy', 'createOrReplaceTablePolicy'].forEach((operationId) =>
    contract.requireResponses(operationId, ['200', '400', '401', '403', '404', '500']),
  );
  ['getTablePolicy', 'dropTablePolicy'].forEach((operationId) =>
    contract.requireResponses(operationId, ['200', '401', '403', '404', '500']),
  );
  ['createTablePolicy', 'createOrReplaceTablePolicy'].forEach((operationId) =>
    contract.requireResponses(operationId, ['409']),
  );
  contract.checkSpec(
    contract.spec.info.version === '0.3.0' &&
      contract.spec.info.description.toLowerCase().includes('experimental'),
    'The management contract must be versioned 0.3.0 and marked experimental',
  );

  const assignmentFields = [
    'resource',
    'scope',
    'access',
    'principal',
    'expireTime',
    'inheritedFrom',
  ];
  contract.requireProperties('PermissionAssignment', assignmentFields);
  contract.requireRequiredProperties('PermissionAssignment', [
    'resource',
    'scope',
    'access',
    'principal',
  ]);
  const grantProperties = contract.requireProperties('GrantPermissionRequest', [
    'resource',
    'scope',
    'access',
    'principal',
    'expireTime',
  ]);
  contract.requireRequiredProperties('GrantPermissionRequest', [
    'resource',
    'access',
    'principal',
  ]);
  contract.checkSpec(
    grantProperties.scope.default === 'SELF',
    'GrantPermissionRequest.scope must default to SELF',
  );
  ['inheritedFrom', 'columns', 'policy', 'grantOption'].forEach((field) =>
    contract.checkSpec(
      !grantProperties[field],
      `Schema GrantPermissionRequest must omit field: ${field}`,
    ),
  );
  const revokeProperties = contract.requireProperties('RevokePermissionRequest', [
    'resource',
    'scope',
    'access',
    'principal',
  ]);
  contract.requireRequiredProperties('RevokePermissionRequest', [
    'resource',
    'access',
    'principal',
  ]);
  contract.checkSpec(
    revokeProperties.scope.default === 'SELF',
    'RevokePermissionRequest.scope must default to SELF',
  );
  ['expireTime', 'inheritedFrom', 'columns', 'policy', 'grantOption'].forEach((field) =>
    contract.checkSpec(
      !revokeProperties[field],
      `Schema RevokePermissionRequest must omit field: ${field}`,
    ),
  );

  const permissionList = contract.requireProperties('ListPermissionsResponse', [
    'permissions',
    'nextPageToken',
  ]);
  contract.checkSpec(
    permissionList.permissions.type === 'array' &&
      permissionList.permissions.items.$ref === '#/components/schemas/PermissionAssignment',
    'ListPermissionsResponse.permissions must contain PermissionAssignment values',
  );
  contract.requireRequiredProperties('ListPermissionsResponse', ['permissions']);

  contract.requireProperties('PrincipalRef', ['type', 'id']);
  contract.requireRequiredProperties('PrincipalRef', ['type', 'id']);
  requireExactEnum(contract, 'ResourceType', ['CATALOG', 'DATABASE', 'TABLE', 'FUNCTION', 'VIEW']);
  requireExactEnum(contract, 'PermissionScope', ['SELF', 'DESCENDANTS']);
  requireExactEnum(contract, 'PrincipalType', ['USER', 'GROUP', 'ROLE', 'SERVICE']);
  requireExactEnum(contract, 'PolicyType', ['ROW_FILTER', 'COLUMN_MASKING']);

  contract.requireSchemaReference('PolicyRequest', 'oneOf', 'RowFilterPolicyRequest');
  contract.requireSchemaReference('PolicyRequest', 'oneOf', 'ColumnMaskPolicyRequest');
  contract.requireSchemaReference('DataPolicy', 'oneOf', 'RowFilterDataPolicy');
  contract.requireSchemaReference('DataPolicy', 'oneOf', 'ColumnMaskDataPolicy');
  contract.requireProperties('RowFilter', ['functionName', 'functionArguments']);
  contract.requireProperties('ColumnMask', ['functionName', 'onColumn', 'functionArguments']);
  contract.requireRequiredProperties('RowFilter', ['functionName', 'functionArguments']);
  contract.requireRequiredProperties('ColumnMask', [
    'functionName',
    'onColumn',
    'functionArguments',
  ]);
  ['RowFilterPolicyRequest', 'ColumnMaskPolicyRequest'].forEach((schemaName) => {
    const properties = contract.requireProperties(schemaName, [
      'name',
      'toPrincipals',
      'exceptPrincipals',
      'comment',
    ]);
    ['toPrincipals', 'exceptPrincipals'].forEach((field) =>
      contract.checkSpec(
        properties[field].items.$ref === '#/components/schemas/PrincipalRef',
        `${schemaName}.${field} must contain PrincipalRef values`,
      ),
    );
    contract.checkSpec(
      !properties.scope && !properties.type && !properties.resource,
      `${schemaName} must not expose policy scope, type, or path resource`,
    );
    contract.checkSpec(
      !(contract.schema(schemaName).required || []).includes('exceptPrincipals'),
      `${schemaName}.exceptPrincipals must be optional`,
    );
  });
  contract.requireProperties('RowFilterPolicyRequest', ['rowFilter']);
  contract.requireProperties('ColumnMaskPolicyRequest', ['columnMask']);
  contract.checkSpec(
    Array.isArray(contract.schema('PolicyArgument').oneOf) &&
      contract.schema('PolicyArgument').oneOf.length === 2,
    'PolicyArgument must contain exactly one column or constant',
  );
  contract.checkSpec(
    !contract.schema('ConstantPolicyArgument').properties.constant.minLength,
    'ConstantPolicyArgument.constant must allow the empty string',
  );
  const tablePolicyResource = contract.requireProperties('TablePolicyResource', [
    'type',
    'database',
    'table',
  ]);
  contract.checkSpec(
    tablePolicyResource.type.const === 'TABLE',
    'Data policies must use a TABLE attachment resource',
  );
  const policyList = contract.requireProperties('ListPoliciesResponse', [
    'policies',
    'nextPageToken',
  ]);
  contract.checkSpec(
    policyList.policies.items.$ref === '#/components/schemas/DataPolicy',
    'ListPoliciesResponse.policies must contain DataPolicy values',
  );
  contract.requireProperties('GetPolicyResponse', ['policy']);
  contract.requireProperties('ErrorResponse', [
    'message',
    'resourceType',
    'resourceName',
    'code',
  ]);
  contract.requireRequiredProperties('ErrorResponse', ['message', 'code']);

  const permissionParameters = contract
    .requireOperation('listPermissions')
    .parameters.map((parameter) =>
      parameter.$ref ? parameter.$ref.split('/').pop() : parameter.name,
    );
  [
    'ResourceTypeQuery',
    'ScopeQuery',
    'DatabaseQuery',
    'TableQuery',
    'FunctionQuery',
    'ViewQuery',
    'PrincipalTypeQuery',
    'PrincipalQuery',
    'AccessQuery',
    'IncludeInherited',
    'PageToken',
    'MaxResults',
  ].forEach((name) =>
    contract.checkSpec(
      permissionParameters.includes(name),
      `Operation listPermissions is missing query parameter: ${name}`,
    ),
  );
  contract.checkSpec(
    contract.spec.components.parameters.ResourceTypeQuery.required === true,
    'Operation listPermissions must require resourceType',
  );
  const policyParameters = contract
    .requireOperation('listTablePolicies')
    .parameters.map((parameter) =>
      parameter.$ref ? parameter.$ref.split('/').pop() : parameter.name,
    );
  contract.checkSpec(
    !policyParameters.includes('IncludeInherited'),
    'Table policies must not expose includeInherited',
  );
  contract.checkSpec(
    contract.spec.paths[
      '/v1/{prefix}/databases/{database}/tables/{table}/policies/{policyName}'
    ].put,
    'Full policy replacement must use PUT',
  );

  contract.checkSpec(
    contract.operations.size === operationIds.length,
    `The management contract must define exactly ${operationIds.length} operations`,
  );
  resourcePaths.forEach((resourcePath) =>
    contract.checkSpec(
      contract.spec.paths[resourcePath].parameters.some(
        (parameter) => parameter.$ref === '#/components/parameters/Prefix',
      ),
      `Path ${resourcePath} must reuse components.parameters.Prefix`,
    ),
  );
  return contract.operations.size;
}

const catalogOperationCount = validateCatalogOpenApi();
const managementOperationCount = validateManagementOpenApi();
console.log(`Validated REST Catalog OpenAPI contract with ${catalogOperationCount} operations.`);
console.log(
  `Validated REST Management OpenAPI contract with ${managementOperationCount} operations.`,
);
