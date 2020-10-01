CREATE EXTERNAL TABLE IF NOT EXISTS crops.cropsdataindia (
  `state` string,
  `distict` string,
  `cropyear` int,
  `season` string,
  `crop` string,
  `area` bigint,
  `production` decimal 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://data-agriculture/'
TBLPROPERTIES ('has_encrypted_data'='false');
