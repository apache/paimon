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
    'dropTablePolicy',
  ];
  const resourcePaths = [
    '/v1/{prefix}/permissions',
    '/v1/{prefix}/permissions/grant',
    '/v1/{prefix}/permissions/revoke',
    '/v1/{prefix}/databases/{database}/tables/{table}/policies',
    '/v1/{prefix}/databases/{database}/tables/{table}/policies/drop',
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
    '/v1/{prefix}/databases/{database}/tables/{table}/policies/{policyName}',
  ].forEach((resourcePath) =>
    contract.checkSpec(
      !contract.spec.paths[resourcePath],
      `Policies must not be attachable outside tables: ${resourcePath}`,
    ),
  );
  operationIds.forEach(contract.requireOperation);
  ['listPermissions', 'grantPermission', 'revokePermission'].forEach((operationId) =>
    contract.requireResponses(operationId, [
      '200',
      '400',
      '401',
      '403',
      '404',
      '429',
      '500',
      '503',
    ]),
  );
  contract.requireResponses('grantPermission', ['409']);
  ['listTablePolicies', 'createTablePolicy'].forEach((operationId) =>
    contract.requireResponses(operationId, [
      '200',
      '400',
      '401',
      '403',
      '404',
      '429',
      '500',
      '503',
    ]),
  );
  contract.requireResponses('dropTablePolicy', [
    '200',
    '400',
    '401',
    '403',
    '404',
    '429',
    '500',
    '503',
  ]);
  contract.requireResponses('createTablePolicy', ['409']);
  contract.checkSpec(
    contract.spec.info.version === '1.0' &&
      contract.spec.info.description.toLowerCase().includes('experimental'),
    'The management contract must be versioned 1.0 and marked experimental',
  );
  contract.checkSpec(
    !Object.prototype.hasOwnProperty.call(contract.spec, 'security'),
    'The management contract must not require one deployment-specific authentication scheme',
  );

  const assignmentFields = ['resource', 'access', 'principal', 'columns', 'expireTime'];
  contract.requireProperties('PermissionAssignment', assignmentFields);
  contract.requireRequiredProperties('PermissionAssignment', ['resource', 'access', 'principal']);
  const grantProperties = contract.requireProperties('GrantPermissionRequest', [
    'resource',
    'access',
    'principal',
    'columns',
    'expireTime',
  ]);
  contract.requireRequiredProperties('GrantPermissionRequest', [
    'resource',
    'access',
    'principal',
  ]);
  ['policy', 'grantOption'].forEach((field) =>
    contract.checkSpec(
      !grantProperties[field],
      `Schema GrantPermissionRequest must omit field: ${field}`,
    ),
  );
  const revokeProperties = contract.requireProperties('RevokePermissionRequest', [
    'resource',
    'access',
    'principal',
  ]);
  contract.requireRequiredProperties('RevokePermissionRequest', [
    'resource',
    'access',
    'principal',
  ]);
  ['expireTime', 'columns', 'policy', 'grantOption'].forEach((field) =>
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

  const principal = contract.schema('Principal');
  contract.checkSpec(
    principal.type === 'string' && principal.minLength === 1 && principal.maxLength === 128,
    'Principal must be a non-empty string that fits the 128-character persistence identity',
  );
  contract.checkSpec(
    !contract.spec.components.schemas.PrincipalRef &&
      !contract.spec.components.schemas.PrincipalType &&
      !contract.spec.components.parameters.PrincipalTypeQuery,
    'Principal must not expose a separate reference object or type',
  );
  ['PermissionAssignment', 'GrantPermissionRequest', 'RevokePermissionRequest'].forEach(
    (schemaName) =>
      contract.checkSpec(
        contract.requireProperties(schemaName, ['principal']).principal.$ref ===
          '#/components/schemas/Principal',
        `Schema ${schemaName}.principal must reference Principal`,
      ),
  );
  contract.checkSpec(
    contract.spec.components.parameters.PrincipalQuery.schema.$ref ===
      '#/components/schemas/Principal',
    'PrincipalQuery must reference Principal',
  );
  requireExactEnum(contract, 'ResourceType', [
    'CATALOG',
    'CATALOG_ALL',
    'DATABASE',
    'DATABASE_ALL',
    'TABLE',
    'COLUMN',
    'FUNCTION',
    'VIEW',
  ]);
  requireExactEnum(contract, 'PolicyType', ['ROW_FILTER', 'COLUMN_MASKING']);
  Object.entries({
    CatalogResource: 'CATALOG',
    CatalogAllResource: 'CATALOG_ALL',
    DatabaseResource: 'DATABASE',
    DatabaseAllResource: 'DATABASE_ALL',
    TableResource: 'TABLE',
    ColumnResource: 'COLUMN',
    FunctionResource: 'FUNCTION',
    ViewResource: 'VIEW',
    RowFilterPolicyIdentity: 'ROW_FILTER',
    ColumnMaskPolicyIdentity: 'COLUMN_MASKING',
    TablePolicyResource: 'TABLE',
  }).forEach(([schemaName, expectedType]) => {
    const typeProperty = contract.requireProperties(schemaName, ['type']).type;
    contract.checkSpec(
      typeProperty.type === 'string' && typeProperty.const === expectedType,
      `${schemaName}.type must be a typed string constant`,
    );
  });

  const permissionAccess = contract.schema('PermissionAccess');
  const expectedAccesses = [
    'ALL',
    'CREATEDATABASE',
    'DESCRIBE',
    'ALTER',
    'DROP',
    'CREATETABLE',
    'CREATEFUNCTION',
    'CREATEVIEW',
    'LIST',
    'SELECT',
    'UPDATE',
    'GRANT',
  ];
  contract.checkSpec(
    permissionAccess.type === 'string' &&
      permissionAccess.enum.length === expectedAccesses.length &&
      expectedAccesses.every((access) => permissionAccess.enum.includes(access)),
    'PermissionAccess must define the complete data access enum',
  );
  contract.checkSpec(
    permissionAccess.maxLength === 32,
    'PermissionAccess must fit the 32-character persistence field',
  );
  ['PermissionAssignment', 'GrantPermissionRequest', 'RevokePermissionRequest'].forEach(
    (schemaName) =>
      contract.checkSpec(
        contract.requireProperties(schemaName, ['access']).access.$ref ===
          '#/components/schemas/PermissionAccess',
        `Schema ${schemaName}.access must reference PermissionAccess`,
      ),
  );
  contract.checkSpec(
    contract.spec.components.parameters.AccessQuery.schema.$ref ===
      '#/components/schemas/PermissionAccess',
    'AccessQuery must reference PermissionAccess',
  );
  contract.requireSchemaReference('PermissionResource', 'oneOf', 'ColumnResource');
  contract.requireSchemaReference('PermissionResource', 'oneOf', 'CatalogAllResource');
  contract.requireSchemaReference('PermissionResource', 'oneOf', 'DatabaseAllResource');
  const catalogAllResource = contract.requireProperties('CatalogAllResource', ['type']);
  contract.requireRequiredProperties('CatalogAllResource', ['type']);
  contract.checkSpec(
    catalogAllResource.type.const === 'CATALOG_ALL',
    'CatalogAllResource must use the CATALOG_ALL discriminator',
  );
  const databaseAllResource = contract.requireProperties('DatabaseAllResource', [
    'type',
    'database',
  ]);
  contract.requireRequiredProperties('DatabaseAllResource', ['type', 'database']);
  contract.checkSpec(
    databaseAllResource.type.const === 'DATABASE_ALL',
    'DatabaseAllResource must use the DATABASE_ALL discriminator',
  );
  const columnResource = contract.requireProperties('ColumnResource', [
    'type',
    'database',
    'table',
  ]);
  contract.requireRequiredProperties('ColumnResource', ['type', 'database', 'table']);
  contract.checkSpec(
    columnResource.type.const === 'COLUMN',
    'ColumnResource must use the COLUMN discriminator',
  );
  const permissionColumns = contract.requireProperties('PermissionColumns', [
    'columnNames',
    'excludedColumnNames',
  ]);
  const permissionColumnsDescription = contract
    .schema('PermissionColumns')
    .description.toLowerCase()
    .replace(/\s+/g, ' ');
  ['intersected', 'fails the query', 'query authorization'].forEach((phrase) =>
    contract.checkSpec(
      permissionColumnsDescription.includes(phrase),
      `PermissionColumns semantics are missing: ${phrase}`,
    ),
  );
  ['columnNames', 'excludedColumnNames'].forEach((field) => {
    const definition = permissionColumns[field];
    contract.checkSpec(
      definition.type === 'array' &&
        definition.minItems === 1 &&
        definition.uniqueItems === true &&
        definition.items.type === 'string' &&
        definition.items.minLength === 1,
      `PermissionColumns.${field} must be a non-empty unique string array`,
    );
  });
  const columnAlternatives = contract.schema('PermissionColumns').oneOf || [];
  ['columnNames', 'excludedColumnNames'].forEach((field) =>
    contract.checkSpec(
      columnAlternatives.some(
        (alternative) =>
          alternative.required &&
          alternative.required.length === 1 &&
          alternative.required[0] === field,
      ),
      `PermissionColumns must define the ${field} alternative`,
    ),
  );
  ['PermissionAssignment', 'GrantPermissionRequest'].forEach((schemaName) => {
    const properties = contract.requireProperties(schemaName, ['columns']);
    contract.checkSpec(
      properties.columns.$ref === '#/components/schemas/PermissionColumns',
      `${schemaName}.columns must reference PermissionColumns`,
    );
    contract.requireSchemaReference(schemaName, 'allOf', 'ColumnAssignmentConstraint');
  });
  ['DatabaseQuery', 'TableQuery', 'FunctionQuery', 'ViewQuery'].forEach((parameterName) => {
    const locatorQuery = contract.spec.components.parameters[parameterName];
    contract.checkSpec(
      locatorQuery.schema.type === 'string' && locatorQuery.schema.minLength === 1,
      `${parameterName} must be a non-empty string`,
    );
  });

  ['TooManyRequests', 'ServiceUnavailable'].forEach((responseName) => {
    const response = contract.spec.components.responses[responseName];
    contract.checkSpec(response, `Missing reusable response: ${responseName}`);
    contract.checkSpec(
      response.headers['Retry-After'].$ref === '#/components/headers/RetryAfter',
      `Response ${responseName} must expose the optional Retry-After header`,
    );
  });
  contract.checkSpec(
    contract.spec.components.headers.RetryAfter.schema.type === 'string',
    'RetryAfter must allow HTTP delta-seconds or an HTTP date as a string',
  );

  const expireTime = contract.schema('ExpireTime');
  contract.checkSpec(
    expireTime.type === 'string' &&
      expireTime.format === 'date-time' &&
      expireTime.pattern === '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,3})?Z$',
    'ExpireTime must use UTC Z with at most three fractional digits',
  );
  ['PermissionAssignment', 'GrantPermissionRequest'].forEach((schemaName) => {
    const expiry = contract.requireProperties(schemaName, ['expireTime']).expireTime;
    contract.checkSpec(
      expiry.$ref === '#/components/schemas/ExpireTime',
      `${schemaName}.expireTime must reference the shared ExpireTime schema`,
    );
  });
  const expireTimePattern = new RegExp(expireTime.pattern);
  ['2027-01-01T00:00:00Z', '2027-01-01T00:00:00.1Z', '2027-01-01T00:00:00.123Z'].forEach(
    (value) =>
      contract.checkSpec(expireTimePattern.test(value), `ExpireTime must accept ${value}`),
  );
  ['2027-01-01T00:00:00.123456Z', '2027-01-01T00:00:00+00:00'].forEach((value) =>
    contract.checkSpec(!expireTimePattern.test(value), `ExpireTime must reject ${value}`),
  );
  const expiryDescription = expireTime.description.toLowerCase();
  contract.checkSpec(
    expiryDescription.includes('exclusive') &&
      expiryDescription.includes('server clock') &&
      expiryDescription.includes('millisecond') &&
      expiryDescription.includes('must not authorize'),
    'expireTime must define exclusive millisecond server-clock authorization semantics',
  );
  const resourceDescription = contract.schema('PermissionResource').description.toLowerCase();
  ['stable internal resource identity', 'renaming', 'dropping', 'recreating'].forEach((phrase) =>
    contract.checkSpec(
      resourceDescription.includes(phrase),
      `PermissionResource lifecycle is missing: ${phrase}`,
    ),
  );
  const policyDescription = contract.schema('DataPolicy').description.toLowerCase();
  ['logical and', 'same column', 'fail closed'].forEach((phrase) =>
    contract.checkSpec(
      policyDescription.includes(phrase),
      `DataPolicy composition is missing: ${phrase}`,
    ),
  );

  contract.requireSchemaReference('PolicyRequest', 'oneOf', 'RowFilterPolicyRequest');
  contract.requireSchemaReference('PolicyRequest', 'oneOf', 'ColumnMaskPolicyRequest');
  contract.requireSchemaReference('DataPolicy', 'oneOf', 'RowFilterDataPolicy');
  contract.requireSchemaReference('DataPolicy', 'oneOf', 'ColumnMaskDataPolicy');
  const rowFilter = contract.requireProperties('RowFilter', ['predicate']);
  const columnMask = contract.requireProperties('ColumnMask', ['onColumn', 'transform']);
  contract.requireRequiredProperties('RowFilter', ['predicate']);
  contract.requireRequiredProperties('ColumnMask', ['onColumn', 'transform']);
  [rowFilter.predicate, columnMask.transform].forEach((definition) => {
    contract.checkSpec(
      definition.type === 'string' &&
        definition.minLength === 1 &&
        definition['x-maxUtf8Bytes'] === 61440 &&
        definition.contentMediaType === 'application/json',
      'Policy definitions must be bounded non-empty JSON strings',
    );
  });
  ['RowFilterPolicyRequest', 'ColumnMaskPolicyRequest'].forEach((schemaName) => {
    const properties = contract.requireProperties(schemaName, ['principal']);
    contract.checkSpec(
      properties.principal.$ref === '#/components/schemas/Principal',
      `${schemaName}.principal must reference Principal`,
    );
    contract.checkSpec(
      !properties.type &&
        !properties.resource &&
        !properties.name &&
        !properties.toPrincipals &&
        !properties.exceptPrincipals,
      `${schemaName} must expose only one principal and no path identity`,
    );
    contract.requireRequiredProperties(schemaName, ['principal']);
  });
  contract.requireProperties('RowFilterPolicyRequest', ['rowFilter']);
  contract.requireProperties('ColumnMaskPolicyRequest', ['columnMask']);
  contract.requireRequiredProperties('RowFilterPolicyRequest', ['rowFilter']);
  contract.requireRequiredProperties('ColumnMaskPolicyRequest', ['columnMask']);
  contract.requireSchemaReference('DropPolicyRequest', 'oneOf', 'RowFilterPolicyIdentity');
  contract.requireSchemaReference('DropPolicyRequest', 'oneOf', 'ColumnMaskPolicyIdentity');
  contract.requireRequiredProperties('RowFilterPolicyIdentity', ['type', 'principal']);
  contract.requireRequiredProperties('ColumnMaskPolicyIdentity', [
    'type',
    'principal',
    'column',
  ]);
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
  ['RowFilterDataPolicy', 'ColumnMaskDataPolicy'].forEach((schemaName) => {
    const properties = contract.requireProperties(schemaName, ['resource', 'principal']);
    contract.checkSpec(
      properties.principal.$ref === '#/components/schemas/Principal',
      `${schemaName}.principal must reference Principal`,
    );
    contract.checkSpec(
      !properties.name && !properties.toPrincipals && !properties.exceptPrincipals,
      `${schemaName} must use a single principal identity`,
    );
  });
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
    'DatabaseQuery',
    'TableQuery',
    'FunctionQuery',
    'ViewQuery',
    'PrincipalQuery',
    'AccessQuery',
    'PageToken',
    'MaxResults',
  ].forEach((name) =>
    contract.checkSpec(
      permissionParameters.includes(name),
      `Operation listPermissions is missing query parameter: ${name}`,
    ),
  );
  contract.checkSpec(
    permissionParameters.length === 9,
    'Operation listPermissions must expose only exact-resource filters and pagination',
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
  ['PolicyTypeQuery', 'PrincipalQuery', 'PolicyColumnQuery', 'PageToken', 'MaxResults'].forEach(
    (name) =>
      contract.checkSpec(
        policyParameters.includes(name),
        `Operation listTablePolicies is missing query parameter: ${name}`,
      ),
  );
  contract.checkSpec(
    !policyParameters.includes('PolicyNameQuery'),
    'Principal-scoped policies must not expose a policy-name filter',
  );
  const tablePoliciesPath =
    contract.spec.paths['/v1/{prefix}/databases/{database}/tables/{table}/policies'];
  const dropTablePolicyPath =
    contract.spec.paths['/v1/{prefix}/databases/{database}/tables/{table}/policies/drop'];
  contract.checkSpec(
    tablePoliciesPath.get &&
      tablePoliciesPath.post &&
      !tablePoliciesPath.put &&
      !tablePoliciesPath.delete,
    'The table policy collection must expose only list and strict creation',
  );
  contract.checkSpec(
    dropTablePolicyPath.post && !dropTablePolicyPath.delete,
    'Policy deletion must use a body-bearing POST action instead of DELETE',
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
