CREATE TABLE IF NOT EXISTS `RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT`(
                                                                                `ITEM`                 STRING,
                                                                                `UDA_ID`               DECIMAL(5,0),
    `UDA_VALUE_ID`         DECIMAL(3,0),
    `LANG`                 DECIMAL(6,0),
    `TRANSLATED_VALUE`     STRING,
    `LAST_UPDATE_ID`       STRING,
    `CREATE_DATETIME`      TIMESTAMP(3),
    `LAST_UPDATE_DATETIME` TIMESTAMP(3),
    `KAFKA_PROCESS_TIME` AS PROCTIME()
    ) WITH (
          'connector' = 'kafka',
          'properties.acks' = '-1',
          'properties.allow.auto.create.topics' = 'true',
          'topic' = '${topic-a}',
          'properties.bootstrap.servers' = '${mdm-auto-kafka-bootstrap-servers}',
          'value.format' = 'changelog-json',
          'properties.group.id' = '${group}',
          'key.fields' = 'ITEM;UDA_ID;LANG',
          'key.format'='json',
          'scan.startup.mode' = 'earliest-offset',
          'value.changelog-json.timestamp-format.standard'='ISO-8601',
          'value.changelog-json.ignore-parse-errors' = 'true'
          );

CREATE TABLE IF NOT EXISTS `RETEK_UDA_ITEM_FF_PRODUCT_ENRICHMENT`(
                                                                     `ITEM`                 STRING,
                                                                     `UDA_ID`               DECIMAL(5,0),
    `UDA_TEXT`             STRING,
    `LAST_UPDATE_ID`       STRING,
    `CREATE_DATETIME`      TIMESTAMP(3),
    `LAST_UPDATE_DATETIME` TIMESTAMP(3),
    `KAFKA_PROCESS_TIME` AS PROCTIME()
    ) WITH (
          'connector' = 'kafka',
          'properties.acks' = '-1',
          'properties.allow.auto.create.topics' = 'true',
          'topic' = '${topic-b}',
          'properties.bootstrap.servers' = '${mdm-auto-kafka-bootstrap-servers}',
          'value.format' = 'changelog-json',
          'properties.group.id' = '${group}',
          'key.fields' = 'ITEM;UDA_ID',
          'key.format'='json',
          'scan.startup.mode' = 'earliest-offset',
          'value.changelog-json.timestamp-format.standard'='ISO-8601',
          'value.changelog-json.ignore-parse-errors' = 'true'
          );

CREATE TABLE IF NOT EXISTS `RETEK_UDA_ITEM_FF_PRODUCT_ENRICHMENT_DIM`(
                                                                         `ITEM`                 STRING,
                                                                         `UDA_ID`               DECIMAL(5,0),
    `UDA_TEXT`             STRING,
    `LAST_UPDATE_ID`       STRING,
    `CREATE_DATETIME`      TIMESTAMP(3),
    `LAST_UPDATE_DATETIME` TIMESTAMP(3),
    `KAFKA_PROCESS_TIME` AS PROCTIME()
    ) WITH (
          'connector' = 'kafka',
          'properties.acks' = '-1',
          'properties.allow.auto.create.topics' = 'true',
          'topic' = '${topic-c}',
          'properties.bootstrap.servers' = '${mdm-auto-kafka-bootstrap-servers}',
          'value.format' = 'changelog-json',
          'properties.group.id' = '${group}',
          'key.fields' = 'ITEM;UDA_ID',
          'key.format'='json',
          'scan.startup.mode' = 'latest-offset',
          'value.changelog-json.timestamp-format.standard'='ISO-8601',
          'value.changelog-json.ignore-parse-errors' = 'true'
          );

CREATE TABLE IF NOT EXISTS `MDM_DIM_LANG_LOOKUPMAP_ORACLE`(
                                                              `LANG`           DECIMAL(6,0) ,
    `ISO_CODE`         STRING
    ) WITH (
          'connector' = 'jdbc',
          'url' = '${mdm-auto-oracle-url}',
          'username' = '${mdm-auto-oracle-username}',
          'password' = '${mdm-auto-oracle-password}',
          'lookup.cache.max-rows' = '1000',
          'lookup.cache.ttl' = '1d',
          'table-name' = 'LANG'
          );

CREATE TABLE IF NOT EXISTS `MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL`(
                                                                            `BU_CODE`      STRING,
                                                                            `ATTRIB_ID`    STRING,
                                                                            `ATTRIB_TYPE`  STRING,
                                                                            `CONTROL_TYPE` STRING
) WITH (
      'connector' = 'jdbc',
      'url' = '${mdm-auto-mysql-product-url}',
      'username' = '${mdm-auto-mysql-username}',
      'password' = '${mdm-auto-mysql-password}',
      'lookup.cache.max-rows' = '1000',
      -- DAYS： "d", "day", HOURS： "h", "hour",MINUTES： "min", "minute",SECONDS： "s", "sec", "second"
      'lookup.cache.ttl' = '1d',
      'table-name' = 'PRODUCT_ATTRIB_TYPE'
      );

CREATE TABLE IF NOT EXISTS `MDM_DIM_UDA_ITEM_FF_LOOKUPMAP_ORACLE`(
                                                                     `ITEM`                 STRING,
                                                                     `UDA_ID`               DECIMAL(5,0),
    `UDA_TEXT`             STRING,
    `LAST_UPDATE_ID`       STRING,
    `CREATE_DATETIME`      TIMESTAMP(3),
    `LAST_UPDATE_DATETIME` TIMESTAMP(3)
    ) WITH (
          'connector' = 'jdbc',
          'url' = '${mdm-auto-oracle-url}',
          'username' = '${mdm-auto-oracle-username}',
          'password' = '${mdm-auto-oracle-password}',
          'lookup.cache.max-rows' = '1000',
          -- DAYS： "d", "day", HOURS： "h", "hour",MINUTES： "min", "minute",SECONDS： "s", "sec", "second"
          'lookup.cache.ttl' = '1d',
          'table-name' = 'UDA_ITEM_FF'
          );

CREATE TABLE IF NOT EXISTS `MDM_DIM_UDA_LOOKUPMAP_ORACLE`(
                                                             `UDA_ID`           DECIMAL(5,0) ,
    `UDA_DESC`         STRING,
    `DISPLAY_TYPE`     STRING,
    `SINGLE_VALUE_IND` STRING
    ) WITH (
          'connector' = 'jdbc',
          'url' = '${mdm-auto-oracle-url}',
          'username' = '${mdm-auto-oracle-username}',
          'password' = '${mdm-auto-oracle-password}',
          'lookup.cache.max-rows' = '1000',
          -- DAYS： "d", "day", HOURS： "h", "hour",MINUTES： "min", "minute",SECONDS： "s", "sec", "second"
          'lookup.cache.ttl' = '1d',
          'table-name' = 'UDA'
          );

CREATE TABLE IF NOT EXISTS `MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM`(
                                                                                    `ITEM`                 STRING,
                                                                                    `UDA_ID`               DECIMAL(5,0),
    `UDA_VALUE_ID`         DECIMAL(3,0),
    `LANG`                 DECIMAL(6,0),
    `TRANSLATED_VALUE`     STRING,
    `LAST_UPDATE_ID`       STRING,
    `CREATE_DATETIME`      TIMESTAMP(3),
    `LAST_UPDATE_DATETIME` TIMESTAMP(3)
    ) WITH (
          'connector' = 'jdbc',
          'url' = '${mdm-auto-oracle-url}',
          'username' = '${mdm-auto-oracle-username}',
          'password' = '${mdm-auto-oracle-password}',
          'lookup.cache.max-rows' = '1000',
          'lookup.cache.ttl' = '1d',
          'table-name' = 'XX_ITEM_ATTR_TRANSLATE'
          );

CREATE VIEW IF NOT EXISTS `MDM_VIEW_PRODUCT_ENRICHMENT_TRANSLATE` AS
    (
    SELECT
    'WTCTH' BU_CODE,
    b.ITEM PRODUCT_ID,
    CAST(CAST(b.UDA_ID AS DECIMAL(5, 0)) AS STRING) ENRICHMENT_ID,
    c.ISO_CODE LANG,
    b.TRANSLATED_VALUE ENRICHMENT_VALUE,
    b.LAST_UPDATE_DATETIME LAST_UPDATED
    FROM
    RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT b
    JOIN MDM_DIM_LANG_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS c
    ON
    CAST(b.LANG AS DECIMAL(6, 0)) = c.LANG
    JOIN MDM_DIM_UDA_ITEM_FF_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS uif
    ON
    uif.ITEM = b.ITEM
    AND CAST(b.UDA_ID AS DECIMAL(5, 0)) = uif.UDA_ID
    JOIN MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL FOR SYSTEM_TIME AS OF b.KAFKA_PROCESS_TIME AS pat
    ON
    CAST(CAST(b.UDA_ID AS DECIMAL(5, 0)) AS STRING) = pat.ATTRIB_ID
    AND pat.BU_CODE = 'WTCTH'
    AND pat.ATTRIB_TYPE = 'PRODUCT_ENRICHMENT'


    UNION ALL



    SELECT
    'WTCTH' BU_CODE,
    uif.ITEM PRODUCT_ID,
    CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) ENRICHMENT_ID,
    c.ISO_CODE LANG,
    b.TRANSLATED_VALUE ENRICHMENT_VALUE,
    uif.LAST_UPDATE_DATETIME LAST_UPDATED
    FROM
    RETEK_UDA_ITEM_FF_PRODUCT_ENRICHMENT_DIM uif
    JOIN MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS b
    ON
    uif.ITEM = b.ITEM
    AND CAST(uif.UDA_ID AS DECIMAL(5, 0)) = b.UDA_ID
    JOIN MDM_DIM_LANG_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS c
    ON
    CAST(b.LANG AS DECIMAL(6, 0)) = c.LANG
    JOIN MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS pat
    ON
    CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) = pat.ATTRIB_ID
    AND pat.ATTRIB_TYPE = 'PRODUCT_ENRICHMENT'
);

CREATE VIEW IF NOT EXISTS `MDM_VIEW_PRODUCT_ENRICHMENT` AS
    (
    SELECT
    'WTCTH' BU_CODE,
    'WTCTH' FORMULA_COUNTRY_ID,
    uif.ITEM PRODUCT_ID,
    CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) ENRICHMENT_ID,
    uif.UDA_TEXT ENRICHMENT_VALUE,
    u.UDA_DESC ENRICHMENT_DESC,
    u.SINGLE_VALUE_IND SINGLE_VALUE_IND,
    u.DISPLAY_TYPE CONTROL_TYPE,
    'EN' LANG,
    'FLINKJDBC' CREATE_BY,
    uif.LAST_UPDATE_ID LAST_UPDATE_BY,
    uif.CREATE_DATETIME CREATED,
    uif.LAST_UPDATE_DATETIME LAST_UPDATED
    FROM
    RETEK_UDA_ITEM_FF_PRODUCT_ENRICHMENT uif
    JOIN MDM_DIM_UDA_LOOKUPMAP_ORACLE FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS u
    ON
    CAST(uif.UDA_ID AS DECIMAL(5, 0)) = u.UDA_ID
    JOIN MDM_DIM_PRODUCT_ATTRIB_TYPE_LOOKUPMAP_MYSQL FOR SYSTEM_TIME AS OF uif.KAFKA_PROCESS_TIME AS pat
    ON
    CAST(CAST(uif.UDA_ID AS DECIMAL(5, 0)) AS STRING) = pat.ATTRIB_ID
    AND pat.ATTRIB_TYPE = 'PRODUCT_ENRICHMENT'
);

CREATE TABLE IF NOT EXISTS `PROCESSED_MDM_PRODUCT_ENRICHMENT`(
                                                                 `BU_CODE`            STRING,
                                                                 `FORMULA_COUNTRY_ID` STRING,
                                                                 `PRODUCT_ID`         STRING,
                                                                 `ENRICHMENT_ID`      STRING,
                                                                 `ENRICHMENT_VALUE`   STRING,
                                                                 `ENRICHMENT_DESC`    STRING,
                                                                 `SINGLE_VALUE_IND`   STRING,
                                                                 `CONTROL_TYPE`       STRING,
                                                                 `LANG`               STRING,
                                                                 `CREATE_BY`          STRING,
                                                                 `LAST_UPDATE_BY`     STRING,
                                                                 `CREATED`            TIMESTAMP(3),
    `LAST_UPDATED`       TIMESTAMP(3)
    ) WITH (
          'connector' = 'kafka',
          'properties.acks' = '-1',
          'properties.allow.auto.create.topics' = 'true',
          'topic' = '${topic-d}',
          'properties.bootstrap.servers' = '${mdm-auto-kafka-bootstrap-servers}',
          'value.format' = 'changelog-json',
          'properties.group.id' = '${group}',
          'key.fields' = 'PRODUCT_ID;FORMULA_COUNTRY_ID;ENRICHMENT_ID;BU_CODE',
          'key.format'='json',
          'scan.startup.mode' = 'earliest-offset',
          'value.changelog-json.timestamp-format.standard'='ISO-8601',
          'value.changelog-json.ignore-parse-errors' = 'true'
          );


INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT  select * from MDM_VIEW_PRODUCT_ENRICHMENT;
INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PRODUCT_ID, ENRICHMENT_ID, LANG, ENRICHMENT_VALUE,LAST_UPDATED)  select PRODUCT_ID, ENRICHMENT_ID, LANG, ENRICHMENT_VALUE,LAST_UPDATED from MDM_VIEW_PRODUCT_ENRICHMENT_TRANSLATE;

