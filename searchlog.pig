/**
 * Using the Excite search log data and fake users data, determine
 * which age group of users (e.g. 20-29, 30-39, etc) are the most prolific
 * searchers, and which age group uses the biggest words. :-)
 */

-- Load up the search log data
searches = LOAD 's3n://mortar-example-data/tutorial/excite.log.bz2' USING PigStorage('\t') AS (user_id:chararray, timestamp:chararray, query:chararray);

-- Get rid of any searches that are blank or without a user
clean_searches = FILTER searches BY user_id IS NOT NULL AND query IS NOT NULL;

-- Load up the user data
users = LOAD 's3n://mortar-example-data/tutorial/users.txt' USING PigStorage('\t') AS (user_id:chararray, age:int);

-- Bucket the user ages by 20-29, 30-39, etc
users_age_buckets = FOREACH users GENERATE user_id, age_bucket(age) AS age_bucket;

-- Join search data to users
joined = JOIN clean_searches BY user_id, users_age_buckets BY user_id;

-- Group by age bucket
grouped = GROUP joined BY age_bucket;

-- Calculate metrics on each age bucket
age_buckets = FOREACH grouped 
             GENERATE group as age_bucket,
                      COUNT(joined) as num_searches,
                      avg_word_length(joined) as avg_word_length;

-- STORE the results into S3 (results stored in the Hawk sandbox will
-- be removed on a regular basis).
-- We use the pig 'rmf' command to remove any
-- existing results first
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/searches_by_age_bucket;
STORE age_buckets INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/searches_by_age_bucket' USING PigStorage('\t');


--------------------

PYTHON UDFS
--------------------

@outputSchema('age_bucket:chararray')
def age_bucket(age):
    """
    Get the age bucket (e.g. 20-29, 30-39) for an age.
    """
    # round the age down to the nearest ten
    low_age = age - (age % 10)
    high_age = low_age + 9
    print 'Original age: %s, low_age: %s, high_age: %s' % (age, low_age, high_age)
    return '%s - %s' % (low_age, high_age)

@outputSchema('avg_word_length:double')
def avg_word_length(bag):
    """
    Get the average word length in each search.
    """
    num_chars_total = 0
    num_words_total = 0
    for tpl in bag:
        query = tpl[2]
        words = query.split(' ')
        num_words = len(words)
        num_chars = sum([len(word) for word in words])

        num_words_total += num_words
        num_chars_total += num_chars

        # deal with strangely-encoded searches
        # before printing out
        print '%s, %s' % (num_words, num_chars)

    return float(num_chars_total) / float(num_words_total)
