/*
 * Finds trending topics (currently single-words only) by month for a corpus
 * of articles from technology news sites (gigaom, techcrunch, allthingsd).
 *
 * Recommended cluster size with default parameters: 5
 * Approximate running time with recommended cluster size: 25 minutes
 *
 * WARNING: as this script uses very large input records (each is a full webpage), 
 * illustrate may be very slow or fail due to too much data if you select an alias
 * towards the middle or end of the pipeline. If you wish to test, you can get a small dataset
 * of articles from Nate Silver's FiveThirtyEight blog that you you process with a 2-node cluster 
 * by changing the INPUT_PATH parameter to 
 * 's3n://mortar-example-data/common-crawl/fivethirtyeight_crawl/*.gz'
 *
 * The script uses several GROUP, nested FOREACH, and FLATTEN operations, 
 * and at points the implementation may be hard to follow. To see the schema
 * of each relation in the data pipeline, select an alias and use the "describe" command.
 *
 * The corpus was extracted from the Common Crawl (hosted on S3) 
 * using an index by domain maintained by Triv.io.
 */

-- Loads 3.3 GB compressed data by default
-- Change INPUT_PATH to 's3n://mortar-example-data/common-crawl/fivethirtyeight_crawl/*.gz' for ~7 MB
-- Change INPUT_PATH to 's3n://mortar-example-data/common-crawl/tech_sites_crawl/4916.gz' for ~500 MB

%default INPUT_PATH 's3n://mortar-example-data/common-crawl/tech_sites_crawl/*.gz'

-- Only text inside <p> elements from the html is considered by the script
-- However, sometimes <p> elements are used for short messages in addition to articles.
-- To address this, this script only considers paragraphs with at least MIN_WORDS_PER_PARAGRAPH.

%default MIN_WORDS_PER_PARAGRAPH '10'

-- Of the extracted words, only words of at least MIN_WORD_LENGTH letters are considered.
-- Smaller words are still counted towards the number of words in a paragraph however.

%default MIN_WORD_LENGTH '5'

-- How many trending words to list for each month in the final output

%default MAX_NUM_TRENDING_WORDS_PER_PERIOD '25'

-- Jars needed by com.commoncrawl.pig.ArcLoader() to run

REGISTER 's3n://mortar-example-data/common-crawl/jars/httpcore-4.2.2.jar';
REGISTER 's3n://mortar-example-data/common-crawl/jars/jsoup-1.7.2.jar';

-- Load common-crawl webpages

pages   =   LOAD '$INPUT_PATH' 
            USING org.commoncrawl.pig.ArcLoader()
            AS (
                date: chararray, length: long, type: chararray, 
                status_code: int, ip_address: chararray, 
                url: chararray, html: chararray
            );

/*
 * Extract an article date from each page's url, ex. 'http://techcrunch.com/2013/02/13/melodrama' -> ('2013', '02', '13')
 * Preprocess the html: 
 *     Remove newlines (CR and/or LF) since we will be extracting multiline paragraphs in the next steps
 *     Ignore escape sequences (ex. '&nbsp;' or '&quot;' -> ' '). 
 *     (we could unescape them with a python udf, but it is not worth the performance hit)
 * Filter out pages for which a date could not be found
 */

pages_preprocessed      =   FOREACH pages GENERATE
                                get_article_date_from_url(url) AS date, 
                                REPLACE(html, '\\r|\\n|&.*?;', ' ') AS html;
pages_filtered          =   FILTER pages_preprocessed BY (date is not null);

/*
 * Extract paragraphs (text inside <p> tags) from the html and flatten
 *
 * We will be be aggregating by (year, month), so we combine those parts of the date into a single field 
 * called "period" for convenience. The period could be changed to any 
 * string representation of a time interval, so long as its lexicographical ordering is the same 
 * as its chronological ordering (ex. 'yyyy-mm-dd' is ok, but 'mm-dd-yyyy' is not).
 *
 * The schema of each resulting tuple is (period: chararray, paragraph: chararray)
 */

paragraphs              =   FOREACH pages_filtered GENERATE
                                CONCAT(date.year, CONCAT('-', date.month)) AS period, 
                                FLATTEN(
                                    -- non-standard piggybank function from piggybank-for-crawl.jar
                                    -- it is in the piggybank package because at some point we'd like to contribute it
                                    -- to the official piggybank, but haven't gotten around to it yet
                                    org.apache.pig.piggybank.evaluation.string.RegexExtractAllToBag(html, '<p.*?>(.*?)</p>')
                                ) AS paragraph;

/*
 * Lowercase all text, remove html tags, and trim leading and trailing whitespace from each paragraph
 * Tokenize each paragraph into words, and filter out paragraphs with few words 
 * (these are probably miscellaneous text on the page not part of the main text of the article)
 */

paragraph_texts         =   FOREACH paragraphs
                            GENERATE period, TRIM(REPLACE(LOWER(paragraph), '<.*?>', ' ')) AS paragraph;
paragraphs_tokenized    =   FOREACH paragraph_texts
                            GENERATE period, TOKENIZE(paragraph) AS words;
paragraphs_filtered     =   FILTER paragraphs_tokenized
                            BY (words is not null) AND (COUNT(words) >= $MIN_WORDS_PER_PARAGRAPH);

/*
 * Flatten the tokenized paragraphs into a relation of individual words
 * Filter out small words (probably not interesting)
 * Get the total number of occurrences of each word in each period
 */

words                   =   FOREACH paragraphs_filtered 
                            GENERATE FLATTEN(words) AS word: chararray, period;
words_letters_only      =   FOREACH words 
                            GENERATE REPLACE(word, '[^a-z]+', '') AS word, period;
words_filtered          =   FILTER words_letters_only BY (SIZE(word) >= $MIN_WORD_LENGTH);

words_grouped           =   GROUP words_filtered BY (word, period);
word_counts_per_period  =   FOREACH words_grouped GENERATE
                                FLATTEN(group) AS (word, period), 
                                COUNT(words_filtered) AS occurrences;

/*
 * Normalize the word counts against the total number of words in each period, resulting in a word frequency
 * (frequency = probability that a random word in the corpus is the given one)
 * Reflatten the word-period-frequency triples
 */

all_words_by_period         =   GROUP word_counts_per_period BY period;
corpus_total_per_period     =   FOREACH all_words_by_period GENERATE 
                                    group AS period,  
                                    SUM(word_counts_per_period.occurrences) AS occurrences;
words_with_corpus_total     =   JOIN word_counts_per_period BY period, corpus_total_per_period BY period;
word_frequencies_per_period =   FOREACH words_with_corpus_total GENERATE
                                    word_counts_per_period::word AS word, 
                                    word_counts_per_period::period AS period, 
                                    (double)word_counts_per_period::occurrences / (double)corpus_total_per_period::occurrences 
                                        AS frequency: double;

/*
 * Group frequencies by word and order chronologically
 * Then find the "velocity" (whether the frequency of the word is increasing or descreasing, 
 * taking into account both absolute changes and relative changes to the period before and
 * combining them using a weighting formula) of the word for each period.
 */

frequencies_by_word         =   GROUP word_frequencies_per_period BY word;
freqs_by_word_ordered       =   FOREACH frequencies_by_word {
                                    ordered = ORDER word_frequencies_per_period BY period ASC;
                                    GENERATE group AS word, ordered.(period, frequency) AS trend;
                                }
word_velocity_over_time     =   FOREACH freqs_by_word_ordered
                                GENERATE word_velocity_over_time(word, trend);

/*
 * Flatten these word velocities and regroup by period
 * Find the highest-velocity words (trending topics) for each period
 */

word_velocities             =   FOREACH word_velocity_over_time GENERATE FLATTEN(word_velocities);
positive_velocities         =   FILTER word_velocities BY (velocity > 0.0);
pos_velocities_by_period    =   GROUP positive_velocities BY period PARALLEL 1;
trending_words_by_period     =   FOREACH pos_velocities_by_period {
                                    ordered_velocities = ORDER positive_velocities BY velocity DESC;
                                    top_velocities = LIMIT ordered_velocities $MAX_NUM_TRENDING_WORDS_PER_PERIOD;
                                    -- for debugging, change "top_velocities.word"
                                    -- to "top_velocities.(word, frequency, abs_vel, rel_vel, velocity)"
                                    GENERATE group AS period, top_velocities.word AS trending_words;
                                }

-- Remove any existing output and store to S3
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/common_crawl_trending_topics;
STORE trending_words_by_period 
    INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/common_crawl_trending_topics' 
    USING PigStorage();


----------------------

python UDFS
-----------------------

import re
from pig_util import outputSchema
from math import sqrt

# Decorator to help udf's handle null input like Pig does (just ignore it and return null)
def null_if_input_null(fn):
    def wrapped(*args, **kwargs):
        for arg in args:
            if arg is None:
                return None
        for k, v in kwargs.items():
            if v is None:
                return None
        return fn(*args, **kwargs)

    wrapped.__name__ = fn.__name__
    wrapped.__doc__ = fn.__doc__
    wrapped.__dict__.update(fn.__dict__)

    return wrapped

split_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{4})/(\d{2})/(\d{2})/.*')
unified_date_in_url_pattern = re.compile('http(?:s)?://.*/(\d{8})/.*')

# 'http://techcrunch.com/2013/02/13/melodrama' -> ('2013', '02', '13')
# 'http://allthingsd.com/20130213/business_money_yay' -> ('2013', '02', '13')
@outputSchema("date: (year: chararray, month: chararray, day: chararray)")
@null_if_input_null
def get_article_date_from_url(url):
    try:
        split_date = re.search(split_date_in_url_pattern, url)
        if split_date:
            parts = split_date.groups()
            return (parts[0], parts[1], parts[2])

        unified_date = re.search(unified_date_in_url_pattern, url)
        if unified_date:
            date = unified_date.group(1)
            return (date[0:4], date[4:6], date[6:8])
    except:
        return None

    return None

# The history of word frequency for each time-period (yyyy-mm) stored 
# in the relation "freqs_by_word_ordered" in common_crawl_trending_topics.pig skips months where that word did not occur. 
# Therefore, when calculating a word's "frequency velocity", we test whether input tuples are from consecutive months: 
# if they are, we look at them relative to each other; 
# if they are not, we know that the actual preceding month had 0 occurrences of the word.
def consecutive_months(last_year, last_month_of_year, year, month_of_year):
    if (year == last_year and month_of_year == last_month_of_year + 1) or (year == last_year + 1 and last_month_of_year == 12 and month_of_year == 1):
        return True
    return False

# cur = 4.0, prev = 2.0 -> returns 2.0
# cur = 2.0, prev = 4.0 -> returns -2.0
# This keeps proportional increases and decreases on the same weighting scale 
def word_relative_velocity(cur, prev):
    if cur > 0.0 and prev > 0.0:
        # should never be negative even without the abs, but who knows what weirdness floating point arithmetic can cause
        ratio = abs(1.0 + ((cur - prev) / prev))
        return sqrt(ratio) if cur >= prev else -sqrt(1.0/ratio)
    else:
        return None

# Word frequencies are usually very small, so we use a multiplier
# to make weights easoer to interpret when debugging
def word_velocity_weight(abs_vel, rel_vel):
    mult = 1000000.0
    return mult * abs_vel * rel_vel * (-1.0 if abs_vel < 0.0 else 1.0)

# A word's "velocity" is a metric we define to describe how it's frequency is changing over time
# We take into account both absolute changes in frequency and relative changes of frequency
@outputSchema("word_velocities: {t: (word: chararray, period: chararray, frequency: double, abs_vel: double, rel_vel: double, velocity: double)}")
@null_if_input_null
def word_velocity_over_time(word, trend):
    velocities = []

    last_year = 0
    last_month_of_year = 1
    last_frequency = 0.0

    for t in trend:
        year = int(t[0][0:4])
        month_of_year = int(t[0][5:7])
        
        if consecutive_months(last_year, last_month_of_year, year, month_of_year):
            absolute_velocity = t[1] - last_frequency
            relative_velocity = word_relative_velocity(t[1], last_frequency)
        else:
            absolute_velocity = t[1]
            relative_velocity = 1.0

        velocities.append((
                            word, t[0], t[1],
                            absolute_velocity,
                            relative_velocity,
                            word_velocity_weight(absolute_velocity, relative_velocity)
                         ))

        last_year = year
        last_month_of_year = month_of_year
        last_frequency = t[1]

    return velocities

--------------------------
