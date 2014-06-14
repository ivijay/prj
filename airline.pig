/**
 * Using on-time airline data, determine the best and worst airports,
 * as well as the best and worse airlines, independent of where they fly.
 */
 
--Load airline data
raw_data = LOAD 's3://mortar-example-data/airline-data/634173101_T_ONTIME.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
AS (year:int, month: int, unique_carrier:chararray, origin_airport_id:chararray, dest_airport_id:chararray,
dep_delay:int, dep_delay_new: int, arr_delay:int, arr_delay_new:int, cancelled:int);

--Create a penalty on cancelled flights by considering them as having an arrival delay of 5 hours
apply_penalty = FOREACH raw_data GENERATE unique_carrier, origin_airport_id, dest_airport_id, null_to_zero(dep_delay_new) AS dep_delay, create_penalty(arr_delay_new) AS arr_delay;

--Get the average departure delay for each airport
group_by_departure_airport =  GROUP apply_penalty BY (origin_airport_id);
avg_delay_departure_airport = FOREACH group_by_departure_airport GENERATE group, AVG(apply_penalty.dep_delay) AS avg_departure_delay;
--Get the average arrival delay for each airport
group_by_arrival_airport =  GROUP apply_penalty BY (dest_airport_id);
avg_delay_arrival_airport = FOREACH group_by_departure_airport GENERATE group, AVG(apply_penalty.arr_delay) AS avg_arrival_delay;

--Get the worst airports so we can look at them
avg_delay_departure_airport_ordered =  ORDER avg_delay_departure_airport BY avg_departure_delay DESC;
avg_delay_arrival_airport_ordered =  ORDER avg_delay_arrival_airport BY avg_arrival_delay DESC;
worst_departures = LIMIT avg_delay_departure_airport_ordered 20;
worst_arrivals = LIMIT avg_delay_arrival_airport_ordered 20;

--join our flight data to the average arrival delay for the relevant arrival and departure airports
join_arrival_delay = JOIN apply_penalty by origin_airport_id, avg_delay_departure_airport by group;
join_both_delay = JOIN join_arrival_delay by dest_airport_id, avg_delay_arrival_airport by group;

--Calculate a normalized delay score for each flight by taking (arrival_delay - avg_arrival_delay) - avg_departure_delay
--This penalizes an airline for having a larger-than-average arrival delay, and attempts to normalize for flights that leave from delayed versus less delayed airports
normalized_data_final = FOREACH join_both_delay GENERATE unique_carrier, ((arr_delay - avg_arrival_delay) - avg_departure_delay) as normalized_arrival_delay;

--Get the normalized badness for each airline
normalized_data_by_airline = GROUP normalized_data_final BY (unique_carrier);
avg_normalized_delay_airline =  FOREACH normalized_data_by_airline GENERATE group, AVG(normalized_data_final.normalized_arrival_delay) AS normalized_arrival_delay;

sorted_avg_normalized_delay_airline = ORDER avg_normalized_delay_airline by normalized_arrival_delay;

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/worst_arrival_airports;
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/worst_departure_airports;
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/ranked_airlines;
STORE worst_arrivals INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/worst_arrival_airports' USING PigStorage('\t');
STORE worst_departures INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/worst_departure_airports' USING PigStorage('\t');
STORE sorted_avg_normalized_delay_airline INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/ranked_airlines' USING PigStorage('\t');



/* UDF in python */
-------------------


@outputSchema('null_to_zero:double')
def null_to_zero(value):
    if value is None:
        return 0
    return value

@outputSchema('create_penalty:double')
def create_penalty(value):
    if value is None:
        return 300
    return value


