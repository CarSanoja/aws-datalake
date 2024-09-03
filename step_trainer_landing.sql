CREATE EXTERNAL TABLE IF NOT EXISTS stedi_database.step_trainer_landing (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
) 
LOCATION 's3://step-trainer-landing-san/data/'
TBLPROPERTIES ('classification' = 'json');
