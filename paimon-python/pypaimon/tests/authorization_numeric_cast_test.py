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

"""Java numeric REST CAST semantics for authorization and masking."""

import json
from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.common.predicate_json_parser import (
    _apply_predicate_transform, _paimon_type_to_arrow, parse_predicate_to_batch_filter,
)


def _cast(source_type, target_type, array):
    return _apply_predicate_transform({
        'name': 'CAST', 'fieldRef': {'name': 'source', 'type': source_type}, 'type': target_type,
    }, pa.RecordBatch.from_pydict({'source': array}))


def test_floating_narrowing_saturates_before_byte_conversion():
    source = pa.array([128.9, -129.9, 1e30, -1e30, None], type=pa.float64())
    assert _cast('DOUBLE', 'TINYINT', source).to_pylist() == [-128, 127, -1, 0, None]


def test_numeric_decimal_rounds_half_up_and_returns_null_on_precision_overflow():
    source = pa.array([1.005, -1.005, 2.675, 999.995, None], type=pa.float64())
    assert _cast('DOUBLE', 'DECIMAL(5,2)', source).to_pylist() == [
        Decimal('1.01'), Decimal('-1.01'), Decimal('2.68'), None, None]


# Generated with Java CastExecutors / PredicateRemapper on Paimon master
# 8de6fdbda9. Includes the decimal compact/wide and original-executor paths;
# the Double-to-Decimal text was additionally verified on Java 8.
JAVA_CASES = [
    json.loads(
        '{"name":"BIGINT_to_TINYINT","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[922337'
        '2036854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[42949672'
        '97],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BIGINT'
        '"},"type":"TINYINT"},"expected":{"name":"BIGINT_to_TINYINT","results":[0,-1,-128,-1,0,127,-1,0,-1,0,0,0,1,'
        '1,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_SMALLINT","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[92233'
        '72036854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[4294967'
        '297],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BIGIN'
        'T"},"type":"SMALLINT"},"expected":{"name":"BIGINT_to_SMALLINT","results":[0,-1,128,255,256,-129,-257,-3276'
        '8,-1,0,0,0,1,1,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_INT","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[9223372036'
        '854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[4294967297],'
        '[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BIGINT"},"'
        'type":"INT"},"expected":{"name":"BIGINT_to_INT","results":[0,-1,128,255,256,-129,-257,32768,65535,65536,-2'
        '147483648,0,1,536870913,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_FLOAT","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[92233720'
        '36854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[4294967297'
        '],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BIGINT"}'
        ',"type":"FLOAT"},"expected":{"name":"BIGINT_to_FLOAT","results":[-9.223372036854776e+18,9.223372036854776e'
        '+18,128.0,255.0,256.0,-129.0,-257.0,32768.0,65535.0,65536.0,2147483648.0,4294967296.0,4294967296.0,9007200'
        '328482816.0,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_DOUBLE","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[9223372'
        '036854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[429496729'
        '7],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BIGINT"'
        '},"type":"DOUBLE"},"expected":{"name":"BIGINT_to_DOUBLE","results":[-9.223372036854776e+18,9.2233720368547'
        '76e+18,128.0,255.0,256.0,-129.0,-257.0,32768.0,65535.0,65536.0,2147483648.0,4294967296.0,4294967297.0,9007'
        '199791611904.0,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_DECIMAL(5,0)","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[9'
        '223372036854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[429'
        '4967297],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"B'
        'IGINT"},"type":"DECIMAL(5,0)"},"expected":{"name":"BIGINT_to_DECIMAL(5,0)","results":[null,null,"128","255'
        '","256","-129","-257","32768","65535","65536",null,null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_DECIMAL(4,2)","fields":[{"name":"v","type":"BIGINT"}],"rows":[[-9223372036854775808],[9'
        '223372036854775807],[128],[255],[256],[-129],[-257],[32768],[65535],[65536],[2147483648],[4294967296],[429'
        '4967297],[9007199791611905],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"B'
        'IGINT"},"type":"DECIMAL(4,2)"},"expected":{"name":"BIGINT_to_DECIMAL(4,2)","results":[null,null,null,null,'
        'null,null,null,null,null,null,null,null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_TINYINT","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-1'
        '29.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","fi'
        'eldRef":{"index":99,"name":"v","type":"FLOAT"},"type":"TINYINT"},"expected":{"name":"FLOAT_to_TINYINT","re'
        'sults":[1,-1,127,-128,127,-1,-1,-1,-1,0,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_SMALLINT","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-'
        '129.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","f'
        'ieldRef":{"index":99,"name":"v","type":"FLOAT"},"type":"SMALLINT"},"expected":{"name":"FLOAT_to_SMALLINT",'
        '"results":[1,-1,127,128,-129,-1,-1,-1,-1,0,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_INT","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-129.9'
        '],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","fieldR'
        'ef":{"index":99,"name":"v","type":"FLOAT"},"type":"INT"},"expected":{"name":"FLOAT_to_INT","results":[1,-1'
        ',127,128,-129,2147483647,2147483647,2147483647,2147483647,-2147483648,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_BIGINT","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-12'
        '9.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","fie'
        'ldRef":{"index":99,"name":"v","type":"FLOAT"},"type":"BIGINT"},"expected":{"name":"FLOAT_to_BIGINT","resul'
        'ts":[1,-1,127,128,-129,2147483648,2147483648,4294967296,9223372036854775807,-9223372036854775808,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_DECIMAL(4,2)","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.005],[-1.005],[2.675],[-'
        '2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index":99'
        ',"name":"v","type":"FLOAT"},"type":"DECIMAL(4,2)"},"expected":{"name":"FLOAT_to_DECIMAL(4,2)","results":["'
        '1.00","-1.00","2.67","-2.67","99.99",null,null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_DECIMAL(38,0)","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.005],[-1.005],[2.675],['
        '-2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index":9'
        '9,"name":"v","type":"FLOAT"},"type":"DECIMAL(38,0)"},"expected":{"name":"FLOAT_to_DECIMAL(38,0)","results"'
        ':["1","-1","3","-3","100","100","100000002004087730000","99999997781963080000000","10000000150474662000000'
        '00000000",null]}}'
    ),
    json.loads(
        '{"name":"FLOAT_to_DECIMAL(38,18)","fields":[{"name":"v","type":"FLOAT"}],"rows":[[1.005],[-1.005],[2.675],'
        '[-2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index":'
        '99,"name":"v","type":"FLOAT"},"type":"DECIMAL(38,18)"},"expected":{"name":"FLOAT_to_DECIMAL(38,18)","resul'
        'ts":["1.004999995231628400","-1.004999995231628400","2.674999952316284000","-2.674999952316284000","99.994'
        '003295898440000","99.995002746582030000",null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_TINYINT","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.9],[-1.9],[127.9],[128.1],['
        '-129.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","'
        'fieldRef":{"index":99,"name":"v","type":"DOUBLE"},"type":"TINYINT"},"expected":{"name":"DOUBLE_to_TINYINT"'
        ',"results":[1,-1,127,-128,127,-1,-1,-1,-1,0,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_SMALLINT","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.9],[-1.9],[127.9],[128.1],'
        '[-129.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST",'
        '"fieldRef":{"index":99,"name":"v","type":"DOUBLE"},"type":"SMALLINT"},"expected":{"name":"DOUBLE_to_SMALLI'
        'NT","results":[1,-1,127,128,-129,-1,-1,-1,-1,0,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_INT","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-129'
        '.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","fiel'
        'dRef":{"index":99,"name":"v","type":"DOUBLE"},"type":"INT"},"expected":{"name":"DOUBLE_to_INT","results":['
        '1,-1,127,128,-129,2147483647,2147483647,2147483647,2147483647,-2147483648,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_BIGINT","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.9],[-1.9],[127.9],[128.1],[-'
        '129.9],[2147483647.0],[2147483648.5],[4294967297.0],[1e+30],[-1e+30],[null]],"transform":{"name":"CAST","f'
        'ieldRef":{"index":99,"name":"v","type":"DOUBLE"},"type":"BIGINT"},"expected":{"name":"DOUBLE_to_BIGINT","r'
        'esults":[1,-1,127,128,-129,2147483647,2147483648,4294967297,9223372036854775807,-9223372036854775808,null]'
        '}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_DECIMAL(4,2)","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.005],[-1.005],[2.675],'
        '[-2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index":'
        '99,"name":"v","type":"DOUBLE"},"type":"DECIMAL(4,2)"},"expected":{"name":"DOUBLE_to_DECIMAL(4,2)","results'
        '":["1.01","-1.01","2.68","-2.68","99.99",null,null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_DECIMAL(38,0)","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.005],[-1.005],[2.675]'
        ',[-2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index"'
        ':99,"name":"v","type":"DOUBLE"},"type":"DECIMAL(38,0)"},"expected":{"name":"DOUBLE_to_DECIMAL(38,0)","resu'
        'lts":["1","-1","3","-3","100","100","100000000000000000000","99999999999999990000000","1000000000000000000'
        '000000000000",null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_DECIMAL(38,18)","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.005],[-1.005],[2.675'
        '],[-2.675],[99.994],[99.995],[1e+20],[1e+23],[1e+30],[null]],"transform":{"name":"CAST","fieldRef":{"index'
        '":99,"name":"v","type":"DOUBLE"},"type":"DECIMAL(38,18)"},"expected":{"name":"DOUBLE_to_DECIMAL(38,18)","r'
        'esults":["1.005000000000000000","-1.005000000000000000","2.675000000000000000","-2.675000000000000000","99'
        '.994000000000000000","99.995000000000000000",null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"INT_to_DOUBLE","fields":[{"name":"v","type":"INT"}],"rows":[[-2147483648],[2147483647],[1],[null]'
        '],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"DOUBLE"},"expected":{'
        '"name":"INT_to_DOUBLE","results":[-2147483648.0,2147483647.0,1.0,null]}}'
    ),
    json.loads(
        '{"name":"INT_to_FLOAT","fields":[{"name":"v","type":"INT"}],"rows":[[-2147483648],[2147483647],[16777217],'
        '[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"FLOAT"},"expect'
        'ed":{"name":"INT_to_FLOAT","results":[-2147483648.0,2147483648.0,16777216.0,null]}}'
    ),
    json.loads(
        '{"name":"BOOLEAN_to_TINYINT","fields":[{"name":"v","type":"BOOLEAN"}],"rows":[[true],[false],[null]],"tran'
        'sform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"TINYINT"},"expected":{"n'
        'ame":"BOOLEAN_to_TINYINT","results":[1,0,null]}}'
    ),
    json.loads(
        '{"name":"BOOLEAN_to_INT","fields":[{"name":"v","type":"BOOLEAN"}],"rows":[[true],[false],[null]],"transfor'
        'm":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"INT"},"expected":{"name":"BO'
        'OLEAN_to_INT","results":[1,0,null]}}'
    ),
    json.loads(
        '{"name":"BOOLEAN_to_BIGINT","fields":[{"name":"v","type":"BOOLEAN"}],"rows":[[true],[false],[null]],"trans'
        'form":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"BIGINT"},"expected":{"nam'
        'e":"BOOLEAN_to_BIGINT","results":[1,0,null]}}'
    ),
    json.loads(
        '{"name":"BOOLEAN_to_FLOAT","fields":[{"name":"v","type":"BOOLEAN"}],"rows":[[true],[false],[null]],"transf'
        'orm":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"FLOAT"},"expected":{"name"'
        ':"BOOLEAN_to_FLOAT","results":[1.0,0.0,null]}}'
    ),
    json.loads(
        '{"name":"BOOLEAN_to_DOUBLE","fields":[{"name":"v","type":"BOOLEAN"}],"rows":[[true],[false],[null]],"trans'
        'form":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"DOUBLE"},"expected":{"nam'
        'e":"BOOLEAN_to_DOUBLE","results":[1.0,0.0,null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_TINYINT","fields":[{"name":"v","type":"STRING"}],"rows":[[""],["+"],["-"],["."],[".5"],'
        '["1."],["1.99"],["+1.99"],["-1.99"],[" 1"],["1 "],["1e3"],["1.2.3"],["1.a"],["１"],["٢"],["1_0"],["0x10"],['
        '"128"],["-129"],["32768"],["-32769"],["2147483648"],["-2147483649"],["9223372036854775808"],["-92233720368'
        '54775809"],["2147483647.99"],["-2147483648.99"],["9223372036854775807.99"],["-9223372036854775808.99"],[nu'
        'll]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"type":"TINYINT"},"expe'
        'cted":{"name":"STRING_to_TINYINT","results":[{"error":"NumberFormatException"},{"error":"NumberFormatExcep'
        'tion"},{"error":"NumberFormatException"},0,0,1,1,1,-1,{"error":"NumberFormatException"},{"error":"NumberFo'
        'rmatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormat'
        'Exception"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatExce'
        'ption"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatExceptio'
        'n"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},'
        '{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"er'
        'ror":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error"'
        ':"NumberFormatException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_SMALLINT","fields":[{"name":"v","type":"STRING"}],"rows":[[""],["+"],["-"],["."],[".5"]'
        ',["1."],["1.99"],["+1.99"],["-1.99"],[" 1"],["1 "],["1e3"],["1.2.3"],["1.a"],["１"],["٢"],["1_0"],["0x10"],'
        '["128"],["-129"],["32768"],["-32769"],["2147483648"],["-2147483649"],["9223372036854775808"],["-9223372036'
        '854775809"],["2147483647.99"],["-2147483648.99"],["9223372036854775807.99"],["-9223372036854775808.99"],[n'
        'ull]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"type":"SMALLINT"},"ex'
        'pected":{"name":"STRING_to_SMALLINT","results":[{"error":"NumberFormatException"},{"error":"NumberFormatEx'
        'ception"},{"error":"NumberFormatException"},0,0,1,1,1,-1,{"error":"NumberFormatException"},{"error":"Numbe'
        'rFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFor'
        'matException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatE'
        'xception"},{"error":"NumberFormatException"},128,-129,{"error":"NumberFormatException"},{"error":"NumberFo'
        'rmatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormat'
        'Exception"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatExce'
        'ption"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_INT","fields":[{"name":"v","type":"STRING"}],"rows":[[""],["+"],["-"],["."],[".5"],["1.'
        '"],["1.99"],["+1.99"],["-1.99"],[" 1"],["1 "],["1e3"],["1.2.3"],["1.a"],["１"],["٢"],["1_0"],["0x10"],["128'
        '"],["-129"],["32768"],["-32769"],["2147483648"],["-2147483649"],["9223372036854775808"],["-922337203685477'
        '5809"],["2147483647.99"],["-2147483648.99"],["9223372036854775807.99"],["-9223372036854775808.99"],[null]]'
        ',"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"type":"INT"},"expected":{"'
        'name":"STRING_to_INT","results":[{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"err'
        'or":"NumberFormatException"},0,0,1,1,1,-1,{"error":"NumberFormatException"},{"error":"NumberFormatExceptio'
        'n"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},'
        '{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"er'
        'ror":"NumberFormatException"},128,-129,32768,-32769,{"error":"NumberFormatException"},{"error":"NumberForm'
        'atException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},2147483647,-2147483648,{'
        '"error":"NumberFormatException"},{"error":"NumberFormatException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_BIGINT","fields":[{"name":"v","type":"STRING"}],"rows":[[""],["+"],["-"],["."],[".5"],['
        '"1."],["1.99"],["+1.99"],["-1.99"],[" 1"],["1 "],["1e3"],["1.2.3"],["1.a"],["１"],["٢"],["1_0"],["0x10"],["'
        '128"],["-129"],["32768"],["-32769"],["2147483648"],["-2147483649"],["9223372036854775808"],["-922337203685'
        '4775809"],["2147483647.99"],["-2147483648.99"],["9223372036854775807.99"],["-9223372036854775808.99"],[nul'
        'l]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"type":"BIGINT"},"expect'
        'ed":{"name":"STRING_to_BIGINT","results":[{"error":"NumberFormatException"},{"error":"NumberFormatExceptio'
        'n"},{"error":"NumberFormatException"},0,0,1,1,1,-1,{"error":"NumberFormatException"},{"error":"NumberForma'
        'tException"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatExc'
        'eption"},{"error":"NumberFormatException"},{"error":"NumberFormatException"},{"error":"NumberFormatExcepti'
        'on"},{"error":"NumberFormatException"},128,-129,32768,-32769,2147483648,-2147483649,{"error":"NumberFormat'
        'Exception"},{"error":"NumberFormatException"},2147483647,-2147483648,9223372036854775807,-9223372036854775'
        '808,null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_BOOLEAN","fields":[{"name":"v","type":"STRING"}],"rows":[["t"],["TRUE"],["TrUe"],["y"],'
        '["YeS"],["1"],["f"],["FALSE"],["FaLsE"],["n"],["No"],["0"],[" true"],["true "],["\\ttrue"],["on"],["off"],'
        '["2"],[""],["1.0"],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"'
        'type":"BOOLEAN"},"expected":{"name":"STRING_to_BOOLEAN","results":[true,true,true,true,true,true,false,fal'
        'se,false,false,false,false,{"error":"RuntimeException"},{"error":"RuntimeException"},{"error":"RuntimeExce'
        'ption"},{"error":"RuntimeException"},{"error":"RuntimeException"},{"error":"RuntimeException"},{"error":"R'
        'untimeException"},{"error":"RuntimeException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_DECIMAL(4,2)","fields":[{"name":"v","type":"STRING"}],"rows":[["1.005"],["-1.005"],["99'
        '.994"],["99.995"],["9999"],["1e3"],["1E+3"],["1E-3"],[".5"],["1."],["+1.005"],["1_0"],[" 1.005"],["1.005 "'
        '],["１２.３４"],["١٢.٣٤"],["bad"],[""],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","t'
        'ype":"STRING"},"type":"DECIMAL(4,2)"},"expected":{"name":"STRING_to_DECIMAL(4,2)","results":["1.01","-1.01'
        '","99.99",null,null,null,null,"0.00","0.50","1.00","1.01",{"error":"NumberFormatException"},{"error":"Numb'
        'erFormatException"},{"error":"NumberFormatException"},"12.34","12.34",{"error":"NumberFormatException"},{"'
        'error":"NumberFormatException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_DECIMAL(38,0)","fields":[{"name":"v","type":"STRING"}],"rows":[["1.005"],["-1.005"],["9'
        '9.994"],["99.995"],["9999"],["1e3"],["1E+3"],["1E-3"],[".5"],["1."],["+1.005"],["1_0"],[" 1.005"],["1.005 '
        '"],["１２.３４"],["١٢.٣٤"],["bad"],[""],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","'
        'type":"STRING"},"type":"DECIMAL(38,0)"},"expected":{"name":"STRING_to_DECIMAL(38,0)","results":["1","-1","'
        '100","100","9999","1000","1000","0","1","1","1",{"error":"NumberFormatException"},{"error":"NumberFormatEx'
        'ception"},{"error":"NumberFormatException"},"12","12",{"error":"NumberFormatException"},{"error":"NumberFo'
        'rmatException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_DECIMAL(38,18)","fields":[{"name":"v","type":"STRING"}],"rows":[["1.005"],["-1.005"],["'
        '99.994"],["99.995"],["9999"],["1e3"],["1E+3"],["1E-3"],[".5"],["1."],["+1.005"],["1_0"],[" 1.005"],["1.005'
        ' "],["１２.３４"],["١٢.٣٤"],["bad"],[""],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v",'
        '"type":"STRING"},"type":"DECIMAL(38,18)"},"expected":{"name":"STRING_to_DECIMAL(38,18)","results":["1.0050'
        '00000000000000","-1.005000000000000000","99.994000000000000000","99.995000000000000000","9999.000000000000'
        '000000","1000.000000000000000000","1000.000000000000000000","0.001000000000000000","0.500000000000000000",'
        '"1.000000000000000000","1.005000000000000000",{"error":"NumberFormatException"},{"error":"NumberFormatExce'
        'ption"},{"error":"NumberFormatException"},"12.340000000000000000","12.340000000000000000",{"error":"Number'
        'FormatException"},{"error":"NumberFormatException"},null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_TINYINT","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.9'
        '99"],["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["18446744073709'
        '551617.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"i'
        'ndex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"TINYINT"},"expected":{"name":"DECIMAL(38,3)_to_TINYINT'
        '","results":[1,-1,127,-128,0,0,0,1,-1,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_SMALLINT","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.'
        '999"],["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["1844674407370'
        '9551617.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"'
        'index":99,"name":"v","type":"DECIMAL(38,3)"},"type":"SMALLINT"},"expected":{"name":"DECIMAL(38,3)_to_SMALL'
        'INT","results":[1,-1,127,128,-32768,0,0,1,-1,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_INT","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.999"]'
        ',["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["184467440737095516'
        '17.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"index'
        '":99,"name":"v","type":"DECIMAL(38,3)"},"type":"INT"},"expected":{"name":"DECIMAL(38,3)_to_INT","results":'
        '[1,-1,127,128,32768,-2147483648,0,1,-1,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_BIGINT","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.99'
        '9"],["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["184467440737095'
        '51617.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"in'
        'dex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"BIGINT"},"expected":{"name":"DECIMAL(38,3)_to_BIGINT","'
        'results":[1,-1,127,128,32768,2147483648,-9223372036854775808,1,3136633892082024447,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_FLOAT","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.999'
        '"],["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["1844674407370955'
        '1617.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"ind'
        'ex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"FLOAT"},"expected":{"name":"DECIMAL(38,3)_to_FLOAT","res'
        'ults":[1.9989999532699585,-1.9989999532699585,127.9990005493164,128.99899291992188,32769.0,2147483648.0,9.'
        '223372036854776e+18,1.8446744073709552e+19,1.0000000409184788e+35,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_DOUBLE","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.999"],["-1.99'
        '9"],["127.999"],["128.999"],["32768.999"],["2147483648.999"],["9223372036854775808.999"],["184467440737095'
        '51617.999"],["99999999999999999999999999999999999.999"],[null]],"transform":{"name":"CAST","fieldRef":{"in'
        'dex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"DOUBLE"},"expected":{"name":"DECIMAL(38,3)_to_DOUBLE","'
        'results":[1.999,-1.999,127.999,128.999,32768.999,2147483648.999,9.223372036854776e+18,1.8446744073709552e+'
        '19,1e+35,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_DECIMAL(4,2)","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.005"],['
        '"-1.005"],["99.994"],["99.995"],["999.499"],["999.500"],[null]],"transform":{"name":"CAST","fieldRef":{"in'
        'dex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"DECIMAL(4,2)"},"expected":{"name":"DECIMAL(38,3)_to_DEC'
        'IMAL(4,2)","results":["1.01","-1.01","99.99",null,null,null,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_DECIMAL(3,0)","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.005"],['
        '"-1.005"],["99.994"],["99.995"],["999.499"],["999.500"],[null]],"transform":{"name":"CAST","fieldRef":{"in'
        'dex":99,"name":"v","type":"DECIMAL(38,3)"},"type":"DECIMAL(3,0)"},"expected":{"name":"DECIMAL(38,3)_to_DEC'
        'IMAL(3,0)","results":["1","-1","100","100","999",null,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,3)_to_DECIMAL(38,10)","fields":[{"name":"v","type":"DECIMAL(38,3)"}],"rows":[["1.005"]'
        ',["-1.005"],["99.994"],["99.995"],["999.499"],["999.500"],[null]],"transform":{"name":"CAST","fieldRef":{"'
        'index":99,"name":"v","type":"DECIMAL(38,3)"},"type":"DECIMAL(38,10)"},"expected":{"name":"DECIMAL(38,3)_to'
        '_DECIMAL(38,10)","results":["1.0050000000","-1.0050000000","99.9940000000","99.9950000000","999.4990000000'
        '","999.5000000000",null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,10)_to_STRING","fields":[{"name":"v","type":"DECIMAL(38,10)"}],"rows":[["0.0000000123"'
        '],["-0.0000000123"],["0.0000000000"],["10000000000000000000000.0000000000"],[null]],"transform":{"name":"C'
        'AST","fieldRef":{"index":99,"name":"v","type":"DECIMAL(38,10)"},"type":"STRING"},"expected":{"name":"DECIM'
        'AL(38,10)_to_STRING","results":["0.0000000123","-0.0000000123","0.0000000000","10000000000000000000000.000'
        '0000000",null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,10)_to_VARCHAR(3)","fields":[{"name":"v","type":"DECIMAL(38,10)"}],"rows":[["0.0000000'
        '123"],["-0.0000000123"],["0.0000000000"],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":'
        '"v","type":"DECIMAL(38,10)"},"type":"VARCHAR(3)"},"expected":{"name":"DECIMAL(38,10)_to_VARCHAR(3)","resul'
        'ts":["0.0","-0.","0.0",null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_TINYINT_stored_INT","fields":[{"name":"v","type":"BIGINT"}],"rows":[[4294967297],[null]'
        '],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"TINYINT"},"expected":'
        '{"name":"BIGINT_to_TINYINT_stored_INT","results":[1,null]}}'
    ),
    json.loads(
        '{"name":"DOUBLE_to_DECIMAL(4,2)_stored_INT","fields":[{"name":"v","type":"DOUBLE"}],"rows":[[1.9],[null]],'
        '"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"DECIMAL(4,2)"},"expecte'
        'd":{"name":"DOUBLE_to_DECIMAL(4,2)_stored_INT","results":["1.00",null]}}'
    ),
    json.loads(
        '{"name":"BIGINT_to_DECIMAL(38,0)_stored_DOUBLE","fields":[{"name":"v","type":"BIGINT"}],"rows":[[900719925'
        '4740993],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"DOUBLE"},"type":"DEC'
        'IMAL(38,0)"},"expected":{"name":"BIGINT_to_DECIMAL(38,0)_stored_DOUBLE","results":["9007199254740992",null'
        ']}}'
    ),
    json.loads(
        '{"name":"DECIMAL(12,3)_to_INT_stored_INT","fields":[{"name":"v","type":"DECIMAL(12,3)"}],"rows":[["1.999"]'
        ',[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"INT"},"expecte'
        'd":{"name":"DECIMAL(12,3)_to_INT_stored_INT","results":[{"error":"ClassCastException"},null]}}'
    ),
    json.loads(
        '{"name":"INT_to_INT_stored_DECIMAL(12,3)","fields":[{"name":"v","type":"INT"}],"rows":[[1],[null]],"transf'
        'orm":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"DECIMAL(12,3)"},"type":"INT"},"expected":{"n'
        'ame":"INT_to_INT_stored_DECIMAL(12,3)","results":[{"error":"ClassCastException"},null]}}'
    ),
    json.loads(
        '{"name":"INT_to_INT_stored_BOOLEAN","fields":[{"name":"v","type":"INT"}],"rows":[[1],[null]],"transform":{'
        '"name":"CAST","fieldRef":{"index":99,"name":"v","type":"BOOLEAN"},"type":"INT"},"expected":{"name":"INT_to'
        '_INT_stored_BOOLEAN","results":[{"error":"ClassCastException"},null]}}'
    ),
    json.loads(
        '{"name":"INT_to_INT_stored_STRING","fields":[{"name":"v","type":"INT"}],"rows":[[1],[null]],"transform":{"'
        'name":"CAST","fieldRef":{"index":99,"name":"v","type":"STRING"},"type":"INT"},"expected":{"name":"INT_to_I'
        'NT_stored_STRING","results":[{"error":"ClassCastException"},null]}}'
    ),
    json.loads(
        '{"name":"STRING_to_INT_stored_INT","fields":[{"name":"v","type":"STRING"}],"rows":[["1"],[null]],"transfor'
        'm":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"INT"},"type":"INT"},"expected":{"name":"STRING'
        '_to_INT_stored_INT","results":[{"error":"ClassCastException"},null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(18,4)_to_DECIMAL(4,2)_stored_DECIMAL(12,3)","fields":[{"name":"v","type":"DECIMAL(18,4)"}'
        '],"rows":[["1.0050"],[null]],"transform":{"name":"CAST","fieldRef":{"index":99,"name":"v","type":"DECIMAL('
        '12,3)"},"type":"DECIMAL(4,2)"},"expected":{"name":"DECIMAL(18,4)_to_DECIMAL(4,2)_stored_DECIMAL(12,3)","re'
        'sults":["1.01",null]}}'
    ),
    json.loads(
        '{"name":"short_circuit_string_int_AND","fields":[{"name":"id","type":"INT"},{"name":"v","type":"STRING"}],'
        '"rows":[[0,"bad"],[1,"1.9"],[2,null]],"predicate":{"kind":"COMPOUND","function":"AND","children":[{"kind":'
        '"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"INT"}},"function":"EQUAL"'
        ',"literals":[1]},{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":1,"name":"v","type":"STRING'
        '"},"type":"INT"},"function":"EQUAL","literals":[1]}]},"expected":{"name":"short_circuit_string_int_AND","r'
        'esults":[false,true,false]}}'
    ),
    json.loads(
        '{"name":"short_circuit_string_int_OR","fields":[{"name":"id","type":"INT"},{"name":"v","type":"STRING"}],"'
        'rows":[[0,"bad"],[1,"1.9"],[2,null]],"predicate":{"kind":"COMPOUND","function":"OR","children":[{"kind":"L'
        'EAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"INT"}},"function":"EQUAL","'
        'literals":[1]},{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":1,"name":"v","type":"STRING"}'
        ',"type":"INT"},"function":"EQUAL","literals":[1]}]},"expected":{"name":"short_circuit_string_int_OR","resu'
        'lts":[{"error":"NumberFormatException"},true,false]}}'
    ),
    json.loads(
        '{"name":"separate_string_int_filters","fields":[{"name":"id","type":"INT"},{"name":"v","type":"STRING"}],"'
        'rows":[[0,"bad"],[1,"1.9"],[2,null]],"filters":[{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":'
        '{"index":0,"name":"id","type":"INT"}},"function":"EQUAL","literals":[1]},{"kind":"LEAF","transform":{"name'
        '":"CAST","fieldRef":{"index":1,"name":"v","type":"STRING"},"type":"INT"},"function":"EQUAL","literals":[1]'
        '}],"expected":{"name":"separate_string_int_filters","results":[false,true,false]}}'
    ),
    json.loads(
        '{"name":"bad_cast_before_function_TRUE","fields":[{"name":"v","type":"STRING"}],"rows":[["bad"],["1.9"],[n'
        'ull]],"predicate":{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"v","type":"STRIN'
        'G"},"type":"INT"},"function":"TRUE","literals":[]},"expected":{"name":"bad_cast_before_function_TRUE","res'
        'ults":[{"error":"NumberFormatException"},true,true]}}'
    ),
    json.loads(
        '{"name":"bad_cast_before_function_FALSE","fields":[{"name":"v","type":"STRING"}],"rows":[["bad"],["1.9"],['
        'null]],"predicate":{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"v","type":"STRI'
        'NG"},"type":"INT"},"function":"FALSE","literals":[]},"expected":{"name":"bad_cast_before_function_FALSE","'
        'results":[{"error":"NumberFormatException"},false,false]}}'
    ),
    json.loads(
        '{"name":"bad_cast_before_function_EQUAL","fields":[{"name":"v","type":"STRING"}],"rows":[["bad"],["1.9"],['
        'null]],"predicate":{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"v","type":"STRI'
        'NG"},"type":"INT"},"function":"EQUAL","literals":[null]},"expected":{"name":"bad_cast_before_function_EQUA'
        'L","results":[{"error":"NumberFormatException"},false,false]}}'
    ),
    json.loads(
        '{"name":"decimal_overflow_IS_NULL","fields":[{"name":"v","type":"STRING"}],"rows":[["99.994"],["99.995"],['
        '"bad"],[null]],"predicate":{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"v","typ'
        'e":"STRING"},"type":"DECIMAL(4,2)"},"function":"IS_NULL","literals":[]},"expected":{"name":"decimal_overfl'
        'ow_IS_NULL","results":[false,true,{"error":"NumberFormatException"},true]}}'
    ),
    json.loads(
        '{"name":"decimal_overflow_IS_NOT_NULL","fields":[{"name":"v","type":"STRING"}],"rows":[["99.994"],["99.995'
        '"],["bad"],[null]],"predicate":{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"v",'
        '"type":"STRING"},"type":"DECIMAL(4,2)"},"function":"IS_NOT_NULL","literals":[]},"expected":{"name":"decima'
        'l_overflow_IS_NOT_NULL","results":[true,false,{"error":"NumberFormatException"},false]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(18,1)_compact_double_DOUBLE","fields":[{"name":"v","type":"DECIMAL(18,1)"}],"rows":[["900'
        '719925474099.3"],["900719925474099.5"],["-900719925474099.3"],[null]],"transform":{"name":"CAST","fieldRef'
        '":{"index":99,"name":"v","type":"DECIMAL(18,1)"},"type":"DOUBLE"},"expected":{"name":"DECIMAL(18,1)_compac'
        't_double_DOUBLE","results":[900719925474099.2,900719925474099.6,-900719925474099.2,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(18,1)_compact_double_FLOAT","fields":[{"name":"v","type":"DECIMAL(18,1)"}],"rows":[["9007'
        '19925474099.3"],["900719925474099.5"],["-900719925474099.3"],[null]],"transform":{"name":"CAST","fieldRef"'
        ':{"index":99,"name":"v","type":"DECIMAL(18,1)"},"type":"FLOAT"},"expected":{"name":"DECIMAL(18,1)_compact_'
        'double_FLOAT","results":[900719938895872.0,900719938895872.0,-900719938895872.0,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,1)_compact_double_DOUBLE","fields":[{"name":"v","type":"DECIMAL(38,1)"}],"rows":[["900'
        '719925474099.3"],["900719925474099.5"],["-900719925474099.3"],[null]],"transform":{"name":"CAST","fieldRef'
        '":{"index":99,"name":"v","type":"DECIMAL(38,1)"},"type":"DOUBLE"},"expected":{"name":"DECIMAL(38,1)_compac'
        't_double_DOUBLE","results":[900719925474099.2,900719925474099.5,-900719925474099.2,null]}}'
    ),
    json.loads(
        '{"name":"DECIMAL(38,1)_compact_double_FLOAT","fields":[{"name":"v","type":"DECIMAL(38,1)"}],"rows":[["9007'
        '19925474099.3"],["900719925474099.5"],["-900719925474099.3"],[null]],"transform":{"name":"CAST","fieldRef"'
        ':{"index":99,"name":"v","type":"DECIMAL(38,1)"},"type":"FLOAT"},"expected":{"name":"DECIMAL(38,1)_compact_'
        'double_FLOAT","results":[900719938895872.0,900719938895872.0,-900719938895872.0,null]}}'
    ),
]


def source_array(values, type_):
    if pa.types.is_decimal(type_):
        values = [None if v is None else Decimal(v) for v in values]
    return pa.array(values, type=type_)


@pytest.mark.parametrize('case', JAVA_CASES, ids=lambda case: case['name'])
def test_existing_numeric_casts_match_java_golden(case):
    schema = pa.schema([(f['name'], _paimon_type_to_arrow(f['type'])) for f in case['fields']])
    cols = [source_array([row[i] for row in case['rows']], field.type) for i, field in enumerate(schema)]
    batch = pa.RecordBatch.from_arrays(cols, schema=schema)
    if 'transform' in case:
        def execute(rows):
            return _apply_predicate_transform(case['transform'], rows)
    else:
        rule = case.get('predicate') or {'kind': 'COMPOUND', 'function': 'AND', 'children': case['filters']}
        execute = parse_predicate_to_batch_filter(json.dumps(rule))
    if 'parseError' in case['expected']:
        with pytest.raises((ValueError, TypeError, pa.ArrowInvalid)):
            execute(batch.slice(0, 0))
        return
    execute(batch.slice(0, 0))
    for i, expected in enumerate(case['expected']['results']):
        if isinstance(expected, dict) and 'error' in expected:
            with pytest.raises((ValueError, TypeError, pa.ArrowInvalid)):
                execute(batch.slice(i, 1))
        else:
            value = execute(batch.slice(i, 1)).to_pylist()[0]
            if isinstance(value, Decimal):
                value = format(value, 'f')
            assert value == expected, (case['name'], i)


def test_boolean_to_decimal_remains_outside_existing_python_support():
    with pytest.raises(pa.ArrowNotImplementedError):
        _cast('BOOLEAN', 'DECIMAL(4,2)', pa.array([True, False, None]))


def test_rebound_integer_to_boolean_keeps_number_long_value():
    array = pa.array([0.5, 1.9, -0.5, -1.9, 1e30, None], type=pa.float64())
    assert _cast('INT', 'BOOLEAN', array).to_pylist() == [False, True, False, True, True, None]
    with pytest.raises(ValueError):
        _cast('DOUBLE', 'BOOLEAN', array)
