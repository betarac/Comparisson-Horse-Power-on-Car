-- create table
create table table_m3(
    "Dimensions.Height" int,
    "Dimensions.Length" int ,
    "Dimensions.Width" int,
    "Engine Information.Driveline" varchar(255),
    "Engine Information.Engine Type" varchar(255),
    "Engine Information.Hybrid" bool,
    "Engine Information.Number of Forward Gears" int,
    "Engine Information.Transmission" varchar(255),
    "Fuel Information.City mpg" int,
    "Fuel Information.Fuel Type" varchar(255),
    "Fuel Information.Highway mpg" int,
    "Identification.Classification" varchar(255),
    "Identification.ID" varchar(255),
    "Identification.Make" varchar(255),
    "Identification.Model Year" varchar(255),
    "Identification.Year" int,
    "Engine Information.Engine Statistics.Horsepower" int,
    "Engine Information.Engine Statistics.Torque" int
)

-- Copy data
COPY table
FROM 'C:\tmp\P2M3_betara_candra_data_raw.csv'
DELIMITER ','
CSV HEADER;