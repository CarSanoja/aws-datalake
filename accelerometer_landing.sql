CREATE EXTERNAL TABLE IF NOT EXISTS stedi_database.accelerometer_landing_san (
    timeStamp BIGINT,
    user STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
) LOCATION 's3://accelerometer-landing-san/data/'
TBLPROPERTIES ('has_encrypted_data'='false');
