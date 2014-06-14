/*
 * Which US state is home to the highest concentration of coffee snobs?
 *
 * Loads up a week's-worth of tweets from the twitter-gardenhose (https://github.com/mortardata/twitter-gardenhose),
 * searches through them for telltale coffee snob phrases (single origin, la marzocco, etc) and rolls up the 
 * results by US state.
 */

-- Load up all of the JSON-formatted tweets
-- (to use just a single file, switch the load uri to 's3n://twitter-gardenhose-mortar/example')
-- To improve performance, we tell the JsonLoader to only load the fields we need
tweets = LOAD 's3n://twitter-gardenhose-mortar/tweets' 
         USING org.apache.pig.piggybank.storage.JsonLoader('place: map[], text: chararray');

-- Filter to get only tweets that have a location in the US
tweets_with_place = 
    FILTER tweets 
        BY place IS NOT NULL 
       AND place#'country_code' == 'US' 
       AND place#'place_type' == 'city';

-- Parse out the US state name from the location
-- and determine whether this is a coffee tweet.
coffee_tweets = 
    FOREACH tweets_with_place
   GENERATE text, 
            place#'full_name' AS place_name,
            us_state(place#'full_name') AS us_state,
            is_coffee_tweet(text) AS is_coffee_tweet;

-- Filter to make sure we only include results with
-- where we found a US State
with_state = 
    FILTER coffee_tweets
        BY us_state IS NOT NULL;

-- Group the results by US state
grouped = 
    GROUP with_state 
       BY us_state;

-- Calculate the percentage of coffee tweets
-- for each state
coffee_tweets_by_state = 
    FOREACH grouped
   GENERATE group as us_state,
            SUM(with_state.is_coffee_tweet) AS num_coffee_tweets,
            COUNT(with_state) AS num_tweets,
            100.0 * SUM(with_state.is_coffee_tweet) / COUNT(with_state) AS pct_coffee_tweets;

-- filter out small states, where the population
-- of tweets is too small to get a fair reading
small_states_filtered =
    FILTER coffee_tweets_by_state
        BY num_tweets > 50000;

-- Order by percentage to get the largest
-- coffee snobs at the top
ordered_output = 
    ORDER small_states_filtered 
       BY pct_coffee_tweets DESC;

-- Remove any existing output and store the results to S3
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/coffee_tweets;
STORE ordered_output 
  INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/coffee_tweets'
  USING PigStorage('\t');



/* python UDFS */
--------------------



from pig_util import outputSchema

@outputSchema('us_state:chararray')
def us_state(full_place):
    """
    Parse the full place name (e.g. Wheeling, WV) to the state name (WV).
    """
    # find the last comma in the string
    last_comma_idx = full_place.rfind(',')
    if last_comma_idx > 0:
        # grab just the state name
        state = full_place[last_comma_idx+1:].strip() 
        print 'Found state %s in full_place: %s' % (state, full_place)
        return state
    else:
        print 'No state in full_place: %s' % full_place
        return None

COFFEE_SNOB_PHRASES = set((\
    'espresso', 'cappucino', 'macchiato', 'latte', 'cortado', 'pour over', 'barista', 
    'flat white', 'siphon pot', 'woodneck', 'french press', 'arabica', 'chemex', 
    'frothed', 'la marzocco', 'mazzer', 'la pavoni', 'nespresso', 'rancilio silvia', 'hario',
    'intelligentsia', 'counter culture', 'barismo', 'sightglass', 'blue bottle', 'stumptown',
    'single origin', 'coffee beans', 'coffee grinder', 'lavazza', 'coffeegeek'\
))

@outputSchema('is_coffee_tweet:int')
def is_coffee_tweet(text):
    """
    Is the given text indicative of coffee snobbery?
    """
    if not text:
        return 0
    
    lowercased = set(text.lower().split())
    return 1 if any((True for phrase in COFFEE_SNOB_PHRASES if phrase in lowercased)) else 0
