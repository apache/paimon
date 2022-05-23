-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Mimics the update of uv and pv by hour of items in an E-commercial website.
-- Primary keys are related with real time; Each record is about 100 bytes.

CREATE TABLE item_uv_pv_hh_source (
    `item_id` BIGINT,
    `item_name` STRING,
    `item_click_uv_hh` BIGINT,
    `item_click_pv_hh` BIGINT,
    `item_like_uv_hh` BIGINT,
    `item_like_pv_hh` BIGINT,
    `item_cart_uv_hh` BIGINT,
    `item_cart_pv_hh` BIGINT,
    `item_share_uv_hh` BIGINT,
    `item_share_pv_hh` BIGINT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '999999999',
    'fields.item_id.min' = '0',
    'fields.item_id.max' = '9999999',
    'fields.item_click_uv_hh.min' = '0',
    'fields.item_click_uv_hh.max' = '999999999',
    'fields.item_click_pv_hh.min' = '0',
    'fields.item_click_pv_hh.max' = '999999999',
    'fields.item_like_uv_hh.min' = '0',
    'fields.item_like_uv_hh.max' = '999999999',
    'fields.item_like_pv_hh.min' = '0',
    'fields.item_like_pv_hh.max' = '999999999',
    'fields.item_cart_uv_hh.min' = '0',
    'fields.item_cart_uv_hh.max' = '999999999',
    'fields.item_cart_pv_hh.min' = '0',
    'fields.item_cart_pv_hh.max' = '999999999',
    'fields.item_share_uv_hh.min' = '0',
    'fields.item_share_uv_hh.max' = '999999999',
    'fields.item_share_pv_hh.min' = '0',
    'fields.item_share_pv_hh.max' = '999999999'
);

CREATE VIEW item_uv_pv_hh AS
SELECT
    CAST(MINUTE(NOW()) AS INT) AS `hr`,
    `item_id`,
    SUBSTR(`item_name`, 0, MOD(`item_id`, 32) + 64) AS `item_name`,
    `item_click_uv_hh`,
    `item_click_pv_hh`,
    `item_like_uv_hh`,
    `item_like_pv_hh`,
    `item_cart_uv_hh`,
    `item_cart_pv_hh`,
    `item_share_uv_hh`,
    `item_share_pv_hh`,
    NOW() AS `ts`
FROM item_uv_pv_hh_source;

-- __SINK_DDL_BEGIN__

CREATE TABLE IF NOT EXISTS ${SINK_NAME} (
    `hr` INT,
    `item_id` BIGINT,
    `item_name` STRING,
    `item_click_uv_hh` BIGINT,
    `item_click_pv_hh` BIGINT,
    `item_like_uv_hh` BIGINT,
    `item_like_pv_hh` BIGINT,
    `item_cart_uv_hh` BIGINT,
    `item_cart_pv_hh` BIGINT,
    `item_share_uv_hh` BIGINT,
    `item_share_pv_hh` BIGINT,
    `ts` TIMESTAMP(3),
    PRIMARY KEY (`hr`, `item_id`) NOT ENFORCED
) WITH (
    ${DDL_TEMPLATE}
);

-- __SINK_DDL_END__

INSERT INTO ${SINK_NAME} SELECT * FROM item_uv_pv_hh;