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

-- Mimics the insert and update by hour of orders in an E-commercial website.
-- Primary keys are related with real time; Each record is about 1.5K bytes.

CREATE TABLE orders_source (
    `order_id` BIGINT,
    `category_id` BIGINT,
    `category_name` STRING,
    `category_description` STRING,
    `item_id` BIGINT,
    `item_name` STRING,
    `item_description` STRING,
    `item_price` INT,
    `item_currency` STRING,
    `item_tags` STRING,
    `item_amount` INT,
    `item_updatetime` TIMESTAMP(3),
    `buyer_id` BIGINT,
    `buyer_name` STRING,
    `buyer_avatar` STRING,
    `buyer_age` INT,
    `buyer_company` STRING,
    `buyer_email` STRING,
    `buyer_phone` BIGINT,
    `buyer_country` STRING,
    `buyer_city` STRING,
    `buyer_street` STRING,
    `buyer_zipcode` INT,
    `buyer_comments` STRING,
    `buyer_buytime` TIMESTAMP(3),
    `seller_id` BIGINT,
    `seller_name` STRING,
    `seller_avatar` STRING,
    `seller_age` INT,
    `seller_company` STRING,
    `seller_email` STRING,
    `seller_phone` BIGINT,
    `seller_country` STRING,
    `seller_city` STRING,
    `seller_street` STRING,
    `seller_zipcode` INT,
    `seller_comments` STRING,
    `seller_selltime` TIMESTAMP(3),
    `shipping_id` BIGINT,
    `shipping_company` STRING,
    `shipping_status` INT,
    `shipping_comments` STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '999999999',
    'fields.order_id.min' = '0',
    'fields.order_id.max' = '9999999',
    'fields.item_currency.length' = '3',
    'fields.buyer_phone.min' = '10000000000',
    'fields.buyer_phone.max' = '99999999999',
    'fields.buyer_country.length' = '3',
    'fields.seller_phone.min' = '10000000000',
    'fields.seller_phone.max' = '99999999999',
    'fields.seller_country.length' = '3',
    'fields.shipping_status.min' = '0',
    'fields.shipping_status.max' = '9'
);

CREATE VIEW orders AS
SELECT
    CAST(MINUTE(NOW()) AS INT) AS `hr`,
    `order_id`,
    `category_id`,
    SUBSTR(`category_name`, 0, ABS(MOD(`category_id`, 64)) + 64) AS `category_name`,
    SUBSTR(`category_description`, 0, ABS(MOD(`category_id`, 128)) + 128) AS `category_description`,
    `item_id`,
    SUBSTR(`item_name`, 0, ABS(MOD(`item_id`, 64)) + 64) AS `item_name`,
    SUBSTR(`item_description`, 0, ABS(MOD(`item_id`, 128)) + 128) AS `item_description`,
    `item_price`,
    `item_currency`,
    SUBSTR(`item_tags`, 0, ABS(MOD(`item_id`, 32)) + 32) AS `item_tags`,
    `item_amount`,
    `item_updatetime`,
    `buyer_id`,
    SUBSTR(`buyer_name`, 0, ABS(MOD(`buyer_id`, 16)) + 16) AS `buyer_name`,
    SUBSTR(`buyer_avatar`, 0, ABS(MOD(`buyer_id`, 128)) + 128) AS `buyer_avatar`,
    `buyer_age`,
    SUBSTR(`buyer_company`, 0, ABS(MOD(`buyer_id`, 32)) + 32) AS `buyer_company`,
    SUBSTR(`buyer_email`, 0, ABS(MOD(`buyer_id`, 16)) + 16) AS `buyer_email`,
    `buyer_phone`,
    `buyer_country`,
    SUBSTR(`buyer_city`, 0, ABS(MOD(`buyer_id`, 8)) + 8) AS `buyer_city`,
    SUBSTR(`buyer_street`, 0, ABS(MOD(`buyer_id`, 16)) + 32) AS `buyer_street`,
    `buyer_zipcode`,
    SUBSTR(`buyer_comments`, 0, ABS(MOD(`buyer_id`, 128)) + 128) AS `buyer_street`,
    `buyer_buytime`,
    `seller_id`,
    SUBSTR(`seller_name`, 0, ABS(MOD(`seller_id`, 16)) + 16) AS `seller_name`,
    SUBSTR(`seller_avatar`, 0, ABS(MOD(`seller_id`, 128)) + 128) AS `seller_avatar`,
    `seller_age`,
    SUBSTR(`seller_company`, 0, ABS(MOD(`seller_id`, 32)) + 32) AS `seller_company`,
    SUBSTR(`seller_email`, 0, ABS(MOD(`seller_id`, 16)) + 16) AS `seller_email`,
    `seller_phone`,
    `seller_country`,
    SUBSTR(`seller_city`, 0, ABS(MOD(`seller_id`, 8)) + 8) AS `seller_city`,
    SUBSTR(`seller_street`, 0, ABS(MOD(`seller_id`, 16)) + 32) AS `seller_street`,
    `seller_zipcode`,
    SUBSTR(`seller_comments`, 0, ABS(MOD(`seller_id`, 128)) + 128) AS `seller_comments`,
    `seller_selltime`,
    `shipping_id`,
    SUBSTR(`shipping_company`, 0, ABS(MOD(`shipping_id`, 32)) + 32) AS `shipping_company`,
    `shipping_status`,
    SUBSTR(`shipping_comments`, 0, ABS(MOD(`shipping_id`, 128)) + 128) AS `shipping_comments`,
    NOW() AS `ts`
FROM orders_source;

-- __SINK_DDL_BEGIN__

CREATE TABLE IF NOT EXISTS ${SINK_NAME} (
    `hr` INT,
    `order_id` BIGINT,
    `category_id` BIGINT,
    `category_name` STRING,
    `category_description` STRING,
    `item_id` BIGINT,
    `item_name` STRING,
    `item_description` STRING,
    `item_price` INT,
    `item_currency` STRING,
    `item_tags` STRING,
    `item_amount` INT,
    `item_updatetime` TIMESTAMP(3),
    `buyer_id` BIGINT,
    `buyer_name` STRING,
    `buyer_avatar` STRING,
    `buyer_age` INT,
    `buyer_company` STRING,
    `buyer_email` STRING,
    `buyer_phone` BIGINT,
    `buyer_country` STRING,
    `buyer_city` STRING,
    `buyer_street` STRING,
    `buyer_zipcode` INT,
    `buyer_comments` STRING,
    `buyer_buytime` TIMESTAMP(3),
    `seller_id` BIGINT,
    `seller_name` STRING,
    `seller_avatar` STRING,
    `seller_age` INT,
    `seller_company` STRING,
    `seller_email` STRING,
    `seller_phone` BIGINT,
    `seller_country` STRING,
    `seller_city` STRING,
    `seller_street` STRING,
    `seller_zipcode` INT,
    `seller_comments` STRING,
    `seller_selltime` TIMESTAMP(3),
    `shipping_id` BIGINT,
    `shipping_company` STRING,
    `shipping_status` INT,
    `shipping_comments` STRING,
    `ts` TIMESTAMP(3),
    PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
    ${DDL_TEMPLATE}
);

-- __SINK_DDL_END__

INSERT INTO ${SINK_NAME} SELECT * FROM orders;
