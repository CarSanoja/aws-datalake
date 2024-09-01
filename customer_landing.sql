CREATE EXTERNAL TABLE IF NOT EXISTS stedi_database.customer_landing_san (
    serialnumber STRING,
    sharewithpublicasofdate STRING,
    birthday STRING,
    registrationdate BIGINT,
    sharewithresearchasofdate BIGINT,
    customername STRING,
    email STRING,
    lastupdatedate BIGINT,
    phone STRING,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
) LOCATION 's3://customer-landing-san/customer/'
TBLPROPERTIES ('has_encrypted_data'='false');
