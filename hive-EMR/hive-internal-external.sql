
Hive property on EMR:
======
property setting at the time of Cluster Creation:

[
  {
    "Classification": "hive-site",
    "Properties": {
      "hive.blobstore.optimizations.enabled": "false"
    }
  }
]

Creating Internal Table:
=========
create Table createdfromspark_csv(
    incident_number string,
    offense_code int,
    offense_code_group string,
    offense_description string,
    district string,
    reporting_area string,
    shooting string,
    occurred_on_date timestamp,
    year int,
    month int,
    day_of_week string,
    hour int,
    ucr_part string,
    street string,
    lat double,
    long double,
    location string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://yaswanth-aug/';

Create External Table:
=========
create External Table crime_external(
    incident_number string,
    offense_code int,
    offense_code_group string,
    offense_description string,
    district string,
    reporting_area string,
    shooting string,
    occurred_on_date timestamp,
    year int,
    month int,
    day_of_week string,
    hour int,
    ucr_part string,
    street string,
    lat double,
    long double,
    location string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://yaswanth-aug/crime-external-table';

Loading data from Internal to External:
============
INSERT INTO TABLE crime_external SELECT * FROM createdfromspark_csv;



