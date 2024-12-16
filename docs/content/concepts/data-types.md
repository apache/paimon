---
title: "Data Types"
weight: 7
type: docs
aliases:
- /concepts/data-types.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Data Types

A data type describes the logical type of a value in the table ecosystem. It can be used to declare input and/or output types of operations.

All data types supported by Paimon are as follows:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 10%">DataType</th>
      <th class="text-left" style="width: 30%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>Data type of a boolean with a (possibly) three-valued logic of TRUE, FALSE, and UNKNOWN.</code></td>
    </tr>
    <tr>
      <td><code>CHAR</code><br>
          <code>CHAR(n)</code>
      </td>
      <td><code>Data type of a fixed-length character string.</code><br><br>
          <code>The type can be declared using CHAR(n) where n is the number of code points. n must have a value between 1 and 2,147,483,647 (both inclusive). If no length is specified, n is equal to 1. </code>
      </td>
    </tr>
    <tr>
      <td><code>VARCHAR</code><br>
          <code>VARCHAR(n)</code><br><br>
          <code>STRING</code>
      </td>
      <td><code>Data type of a variable-length character string.</code><br><br>
          <code>The type can be declared using VARCHAR(n) where n is the maximum number of code points. n must have a value between 1 and 2,147,483,647 (both inclusive). If no length is specified, n is equal to 1. </code><br><br>
          <code>STRING is a synonym for VARCHAR(2147483647).</code>
      </td>
    </tr>
    <tr>
      <td><code>BINARY</code><br>
          <code>BINARY(n)</code><br><br>
      </td>
      <td><code>Data type of a fixed-length binary string (=a sequence of bytes).</code><br><br>
          <code>The type can be declared using BINARY(n) where n is the number of bytes. n must have a value between 1 and 2,147,483,647 (both inclusive). If no length is specified, n is equal to 1.</code>
      </td>
    </tr>
    <tr>
      <td><code>VARBINARY</code><br>
          <code>VARBINARY(n)</code><br><br>
          <code>BYTES</code>
      </td>
      <td><code>Data type of a variable-length binary string (=a sequence of bytes).</code><br><br>
          <code>The type can be declared using VARBINARY(n) where n is the maximum number of bytes. n must have a value between 1 and 2,147,483,647 (both inclusive). If no length is specified, n is equal to 1.</code><br><br>
          <code>BYTES is a synonym for VARBINARY(2147483647).</code>
      </td>
    </tr>
    <tr>
      <td><code>DECIMAL</code><br>
          <code>DECIMAL(p)</code><br>
          <code>DECIMAL(p, s)</code>
      </td>
      <td><code>Data type of a decimal number with fixed precision and scale.</code><br><br>
          <code>The type can be declared using DECIMAL(p, s) where p is the number of digits in a number (precision) and s is the number of digits to the right of the decimal point in a number (scale). p must have a value between 1 and 38 (both inclusive). s must have a value between 0 and p (both inclusive). The default value for p is 10. The default value for s is 0.</code>
      </td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>Data type of a 1-byte signed integer with values from -128 to 127.</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>Data type of a 2-byte signed integer with values from -32,768 to 32,767.</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>Data type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>Data type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>Data type of a 4-byte single precision floating point number.</code><br><br>
          <code>Compared to the SQL standard, the type does not take parameters.</code>
      </td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>Data type of an 8-byte double precision floating point number.</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>Data type of a date consisting of year-month-day with values ranging from 0000-01-01 to 9999-12-31.</code><br><br>
          <code>Compared to the SQL standard, the range starts at year 0000.</code>
      </td>
    </tr>
    <tr>
      <td><code>TIME</code><br>
          <code>TIME(p)</code>
      </td>
      <td><code>Data type of a time without time zone consisting of hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 00:00:00.000000000 to 23:59:59.999999999.</code><br><br>
          <code>The type can be declared using TIME(p) where p is the number of digits of fractional seconds (precision). p must have a value between 0 and 9 (both inclusive). If no precision is specified, p is equal to 0.</code>
      </td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code><br>
          <code>TIMESTAMP(p)</code>
      </td>
      <td><code>Data type of a timestamp without time zone consisting of year-month-day hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 to 9999-12-31 23:59:59.999999999.</code><br><br>
          <code>The type can be declared using TIMESTAMP(p) where p is the number of digits of fractional seconds (precision). p must have a value between 0 and 9 (both inclusive). If no precision is specified, p is equal to 6.</code>
      </td>
    </tr>
    <tr>
      <td><code>TIMESTAMP WITH TIME ZONE</code><br>
          <code>TIMESTAMP(p) WITH TIME ZONE</code>
      </td>
      <td><code>Data type of a timestamp with time zone consisting of year-month-day hour:minute:second[.fractional] zone with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 +14:59 to 9999-12-31 23:59:59.999999999 -14:59.</code><br><br>
          <code>This type fills the gap between time zone free and time zone mandatory timestamp types by allowing the interpretation of UTC timestamps according to the configured session time zone. A  conversion from and to int describes the number of seconds since epoch. A conversion from and to long describes the number of milliseconds since epoch.</code>
      </td>
    </tr>
    <tr>
      <td><code>ARRAY&lt;t&gt;</code></td>
      <td><code>Data type of an array of elements with same subtype.</code><br><br>
          <code>Compared to the SQL standard, the maximum cardinality of an array cannot be specified but is fixed at 2,147,483,647. Also, any valid type is supported as a subtype.</code><br><br>
          <code>The type can be declared using ARRAY&lt;t&gt; where t is the data type of the contained elements.</code>
      </td>
    </tr>
    <tr>
      <td><code>MAP&lt;kt, vt&gt;</code></td>
      <td><code>Data type of an associative array that maps keys (including NULL) to values (including NULL). A map cannot contain duplicate keys; each key can map to at most one value.</code><br><br>
          <code>There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.</code><br><br>
          <code>The type can be declared using MAP&lt;kt, vt&gt; where kt is the data type of the key elements and vt is the data type of the value elements.</code>
      </td>
    </tr>
    <tr>
      <td><code>MULTISET&lt;t&gt;</code></td>
      <td><code>Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its elements with a common subtype. Each unique value (including NULL) is mapped to some multiplicity.</code><br><br>
          <code>There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.</code><br><br>
          <code>The type can be declared using MULTISET&lt;t&gt; where t is the data type of the contained elements.</code>
      </td>
    </tr>
    <tr>
      <td><code>ROW&lt;n0 t0, n1 t1, ...&gt;</code><br>
          <code>ROW&lt;n0 t0 'd0', n1 t1 'd1', ...&gt;</code>
      </td>
      <td><code>Data type of a sequence of fields.</code><br><br>
          <code>A field consists of a field name, field type, and an optional description. The most specific type of a row of a table is a row type. In this case, each column of the row corresponds to the field of the row type that has the same ordinal position as the column.</code><br><br>
          <code>Compared to the SQL standard, an optional field description simplifies the handling with complex structures.</code><br><br>
          <code>A row type is similar to the STRUCT type known from other non-standard-compliant frameworks.</code><br><br>
          <code>The type can be declared using ROW&lt;n0 t0 'd0', n1 t1 'd1', ...&gt; where n is the unique name of a field, t is the logical type of a field, d is the description of a field.</code>
      </td>
    </tr>
    </tbody>
</table>
