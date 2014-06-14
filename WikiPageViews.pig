/**
 * This script calculates the average number of pageviews
 * across all Wikipedia sites, rolled up by weekday versus weekend.
 *
 * The data comes from: http://dumps.wikimedia.org/other/pagecounts-ez/monthly/
 */
 
-- Load up the pageview data, with one row per wiki and article
raw = LOAD 's3n://mortar-example-data/wikipedia/*' 
     USING PigStorage(' ') 
        AS (wiki_code:chararray, article:chararray, monthly_pageviews:int, encoded_hourly_pageviews:chararray);

-- Use python to decode the hourly pageviews
-- into one row per wiki, article, day, and hour
decoded = FOREACH raw
         GENERATE wiki_code,
                  article,
                  FLATTEN(decode_pageviews(encoded_hourly_pageviews)) AS (day, hour, hourly_pageviews);

-- Group them by day
grouped = GROUP decoded 
             BY day;

-- Determine whether each day is a weekday or not using python
daily = FOREACH grouped 
       GENERATE group AS day,
                is_weekday(2011, 7, group) AS is_weekday,
                SUM(decoded.hourly_pageviews) AS daily_pageviews;

-- Group by whether it was a weekday
daily_grouped = GROUP daily 
                   BY is_weekday;

-- Emit the average for weekday and weekend
results = FOREACH daily_grouped
         GENERATE group as is_weekday,
                  AVG(daily.daily_pageviews) AS avg_pageviews;

-- STORE the results into S3 (results stored in the Hawk sandbox will
-- be removed on a regular basis).
-- We use the pig 'rmf' command to remove any existing results first
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/wikipedia-pageviews-by-weekend-or-weekday;
STORE results INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/wikipedia-pageviews-by-weekend-or-weekday' USING PigStorage('\t');


---------
PYTHON UDFS
----------


import datetime

@outputSchema("decode_pageviews:bag{hourly_pageviews:tuple(day:int, hour:int, pageviews:int)}")
def decode_pageviews(pageview_str):
    """
    Decode the wikipedia pageview string format into
    (day, hour, hourly_pageviews).
    
    See _decode_pageview_str for description of how to parse the wikipedia format.
    """
    encoded_pageviews = pageview_str.split(',')
    return [_decode_pageview_str(encoded_pageview) for encoded_pageview in encoded_pageviews]


@outputSchema("is_weekday:chararray")
def is_weekday(year, month, day):
    """
    Is the given date on a weekday?
    """
    dt = datetime.date(year,month,day)
    # MON=1, TUE=2,...SAT=6, SUN=7
    return 'WEEKDAY' if dt.isoweekday() < 6 else 'WEEKEND'

def _decode_pageview_str(encoded_pageview):
    """
    Pull out the day, hour, and hourly pageviews from the wikipedia log format.
    
    Wikipedia day and hour are coded as one character each, as follows:
    Hour 0..23 shown as A..X                            convert to number: ordinal (char) - ordinal ('A')
    Day  1..31 shown as A.._  27=[ 28=\ 29=] 30=^ 31=_  convert to number: ordinal (char) - ordinal ('A') + 1
    
    Returns (hour, day, num_pageviews)
    """
    day_encoded, hour_encoded, pageviews = \
        (encoded_pageview[0],
         encoded_pageview[1],
         encoded_pageview[2:])
         
    # hour: ordinal (char) - ordinal ('A')
    day_decoded  = ord(day_encoded) - ord('A') + 1
    hour_decoded = ord(hour_encoded) - ord('A')
    
    return (day_decoded, hour_decoded, int(pageviews))
