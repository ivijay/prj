/*
 * Finds which words show up most frequently in tweets expressing positive and negative
 * sentiments relative to the word's frequency in the total corpus of tweets.
 *
 * Recommended cluster size with default parameters: 10
 * Approximate running time with recommended cluster size: 30 minutes
 * (first mapreduce job will take a majority of the time, so the progress meter will be inaccurate)
 *
 * Words are filtered so that only those with length >= MIN_WORD_LENGTH are counted.
 *
 * Postive/negative associations (the words with the greatest relative frequency for positive/negative tweets)
 * are filtered so as not to include the words which signalled positive/negative sentiment in the first place. 
 * This way, you don't just get a list of words like "great" and "awesome".
 * 
 * All text is converted to lower case before being analyzed.
 * Words with non-alphabetic characters in the middle of them are ignored ("C3P0"), 
 * but words with non-alphabetic characters on the edges simply have them stripped ("totally!!!" -> "totally")
 */

%default MIN_WORD_LENGTH '5'

-- for reference:
--     the 8,000'th most-frequent word in the English language has a frequency of ~ 0.00001
--     the 33,000'th most-frequent word in the English language has a frequency of ~ 0.000001
--     the 113,000'th most-frequent word in the English language has a frequency of ~ 0.0000001
%default MIN_ASSOCIATION_FREQUENCY '0.0000005'
%default MAX_NUM_ASSOCIATIONS '100'

-- Defining macros which we will use to calculate word statistics
-- (total # occurrences, frequency relative to own corpus, frequency relative to another corpus)
-- If this were a Mortar project, these macros would be in their own file
-- (you can clone all the mortar example scripts as a Mortar project with
--  git clone git@github.com:mortardata/mortar-examples.git)

/*
 * words_rel: {t: (words: {t: (word: chararray)})}
 * min_length: int
 * ==>
 * word_totals: {t: (word: chararray, occurrences: long)}
 */
DEFINE WORD_TOTALS(words_rel, min_length)
RETURNS word_totals {
    words_flat          =   FOREACH $words_rel GENERATE FLATTEN(words) AS word;
    significant_words   =   FILTER words_flat BY SIZE(word) >= $min_length;
    words_grouped       =   GROUP significant_words BY word;
    $word_totals        =   FOREACH words_grouped GENERATE 
                                group AS word, 
                                COUNT(significant_words) AS occurrences;
};

/*
 * word_counts: {t: (word: chararray, occurrences: long)}
 * ==> 
 * word_frequencies: {t: (word: chararray, occurrences: long, frequency: double)}
 */
DEFINE WORD_FREQUENCIES(word_counts)
RETURNS word_frequencies {
    all_words               =   GROUP $word_counts ALL;
    corpus_total            =   FOREACH all_words GENERATE SUM($word_counts.occurrences) AS occurrences;
    $word_frequencies       =   FOREACH $word_counts GENERATE
                                    $0 AS word, $1 AS occurrences,
                                    (double)$1 / (double)corpus_total.occurrences AS frequency: double;
};

/*
 * subset: {t: (word: chararray, occurrences: long, frequency: double)}
 * corpus: {t: (word: chararray, occurrences: long, frequency: double)}
 * min_corpus_frequency: double
 * ==>
 * rel_frequencies: {
 *                    t: (word: chararray, subset_occurrences: long, corpus_occurrences: long, 
 *                        subset_frequency: double, corpus_frequency: double, rel_frequency: double)
 *                  }
 */
DEFINE RELATIVE_WORD_FREQUENCIES(subset, corpus, min_corpus_frequency)
RETURNS rel_frequencies {
    joined              =   JOIN $subset BY word, $corpus BY word;
    filtered            =   FILTER joined BY ($corpus::frequency > $min_corpus_frequency);
    $rel_frequencies    =   FOREACH filtered GENERATE
                                $subset::word AS word,
                                $subset::occurrences AS subset_occurrences,
                                $corpus::occurrences AS corpus_occurrences,
                                $subset::frequency AS subset_frequency, 
                                $corpus::frequency AS corpus_frequency, 
                                $subset::frequency / $corpus::frequency AS rel_frequency;
};

-- Load tweets
-- To improve performance, we tell the JsonLoader to only load the field that we need (text)

tweets = LOAD 's3n://twitter-gardenhose-mortar/tweets' 
         USING org.apache.pig.piggybank.storage.JsonLoader('text: chararray');

-- Split the text of each tweet into words and calculate a sentiment score

tweets_tokenized        =   FOREACH tweets GENERATE words_from_text(text) AS words;
tweets_with_sentiment   =   FOREACH tweets_tokenized 
                            GENERATE words, sentiment(words) AS sentiment: double;

SPLIT tweets_with_sentiment INTO
    positive_tweets IF (sentiment > 0.0),
    negative_tweets IF (sentiment < 0.0);

-- Find the frequency of each word of at least MIN_WORD_LENGTH letters in all the tweets
-- (frequency = the probability that a random word in the corpus is the given word)
-- The macros used are in macros/words.pig

tweet_word_totals       =   WORD_TOTALS(tweets_tokenized, $MIN_WORD_LENGTH);
tweet_word_frequencies  =   WORD_FREQUENCIES(tweet_word_totals);

-- Find the frequencies of words that show up in tweets expressing positive sentiment, 
-- and divide them by the frequencies of those words in the entire tweet corpus
-- to find the relative frequency of each word. 

pos_word_totals         =   WORD_TOTALS(positive_tweets, $MIN_WORD_LENGTH);
pos_word_frequencies    =   WORD_FREQUENCIES(pos_word_totals);
pos_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(pos_word_frequencies, tweet_word_frequencies, $MIN_ASSOCIATION_FREQUENCY);

-- Take the top 100 of these positively associated words, 
-- filtering out the words which signalled the positive sentiment in the first place (ex. "great", "awesome").

pos_associations        =   ORDER pos_rel_frequencies BY rel_frequency DESC;
pos_assoc_filtered      =   FILTER pos_associations BY (in_word_set(word, 'positive') == 0);
top_pos_associations    =   LIMIT pos_assoc_filtered $MAX_NUM_ASSOCIATIONS;

-- Do the same with negative words.

neg_word_totals         =   WORD_TOTALS(negative_tweets, $MIN_WORD_LENGTH);
neg_word_frequencies    =   WORD_FREQUENCIES(neg_word_totals);
neg_rel_frequencies     =   RELATIVE_WORD_FREQUENCIES(neg_word_frequencies, tweet_word_frequencies, $MIN_ASSOCIATION_FREQUENCY);
neg_associations        =   ORDER neg_rel_frequencies BY rel_frequency DESC;
neg_assoc_filtered      =   FILTER neg_associations BY (in_word_set(word, 'negative') == 0);
top_neg_associations    =   LIMIT neg_assoc_filtered $MAX_NUM_ASSOCIATIONS;

-- Remove any existing output and store to S3

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment/positive;
rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment/negative;
STORE top_pos_associations INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment/positive' USING PigStorage('\t');
STORE top_neg_associations INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_sentiment/negative' USING PigStorage('\t');


--------
PYTHON UDFS
-----------

import re

from collections import defaultdict
from pig_util import outputSchema

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

non_english_character_pattern = re.compile("[^a-z']")

# Accepts strings consisting of 1 or more characters in [a-z']
# (the apostrophe is so that contraction words such as don't are accepted)
def is_alphabetic(s):
    return len(s) > 0 and not bool(non_english_character_pattern.search(s))

whitespace_pattern = re.compile('\\s+')
word_with_punctuation_pattern = re.compile("^[^a-z']*([a-z']+)[^a-z']*$")

# Tokenizes a string into bag of single-element tuples, each containing a single word.
# Strips casing and punctuation (ex. "Totally!!!" -> "totally").
# Excludes words which are not accepted by is_alphabetic after being stripped of punctuation.
@outputSchema("words: {t: (word: chararray)}")
@null_if_input_null
def words_from_text(text):
    return [(word, ) for word in 
            [re.sub(word_with_punctuation_pattern, '\\1', word) 
             for word in re.split(whitespace_pattern, text.lower())
            ] if is_alphabetic(word)]

positive_words = set([
"addicting", "addictingly", "admirable", "admirably", "admire", "admires", "admiring", "adorable", 
"adorably", "adore", "adored", "adoring", "amaze", "amazed", "amazes", "amazing", 
"angelic", "appeal", "appealed", "appealing", "appealingly", "appeals", "attentive", "attracted", 
"attractive", "awesome", "awesomely", "beautiful", "beautifully", "best", "bliss", "bold", 
"boldly", "boss", "bravo", "breath-taking", "breathtaking", "calm", "cared", "cares", 
"caring", "celebrate", "celebrated", "celebrating", "charm", "charmed", "charming", "charmingly", 
"cheer", "cheered", "cheerful", "cheerfully", "classic", "colorful", "colorfully", "colourful", 
"colourfully", "comfort", "comfortably", "comforting", "comfortingly", "comfy", "competent", "competently", 
"congrats", "congratulations", "considerate", "considerately", "cool", "coolest", "courteous", "courteously", 
"creative", "creatively", "cute", "dapper", "dazzled", "dazzling", "dazzlingly", "delicious", 
"deliciously", "delight", "delighted", "delightful", "delightfully", "dope", "dynamic", "ecstatic", 
"efficient", "efficiently", "elegant", "elegantly", "eloquent", "embrace", "embraced", "embracing", 
"energetic", "energetically", "engaging", "engagingly", "enjoy", "enjoyed", "enjoying", "enticing", 
"enticingly", "essential", "excellent", "excellently", "exceptional", "excitement", "exciting", "excitingly", 
"exquisite", "exquisitely", "fantastic", "fascinating", "fashionable", "fashionably", "fast", "favorite", 
"favorites", "favourite", "favourites", "fetching", "fine", "flattering", "fond", "fondly", 
"friendly", "fulfilling", "fun", "generous", "generously", "genius", "genuine", "glamor", 
"glamorous", "glamorously", "glamour", "glamourous", "glamourously", "glorious", "good", "good-looking", 
"goodlooking", "gorgeous", "gorgeously", "grace", "graceful", "gracefully", "great", "handsome", 
"happiness", "happy", "healthy", "heartwarming", "heavenly", "helpful", "hip", "imaginative", 
"incredible", "ingenious", "innovative", "inspirational", "inspired", "inspiring", "intelligent", "interesting", 
"invigorating", "irresistible", "irresistibly", "joy", "kawaii", "keen", "knowledgeable", "liked", 
"lively", "love", "loved", "lovely", "loving", "lucky", "luscious", "lusciously", 
"magical", "magnificent", "marvelous", "marvelously", "masterful", "masterfully", "memorable", "mmm", 
"mmmm", "mmmmm", "natural", "neat", "neatly", "nice", "nicely", "nifty", 
"optimistic", "outstanding", "outstandingly", "overjoyed", "pampered", "peace", "peaceful", "phenomenal", 
"pleasant", "pleasantly", "pleasurable", "pleasurably", "plentiful", "polished", "popular", "positive", 
"powerful", "powerfully", "precious", "prettily", "pretty", "profound", "proud", "proudly", 
"quick", "quickly", "rad", "radiant", "rejoice", "rejoiced", "rejoicing", "remarkable", 
"respectable", "respectably", "respectful", "satisfied", "serenity", "sexily", "sexy", "shiny", 
"skilled", "skillful", "slick", "smooth", "spectacular", "spicy", "splendid", "straightforward", 
"stunning", "stylish", "stylishly", "sublime", "succulent", "super", "superb", "swell", 
"tastily", "tasty", "terrific", "thorough", "thrilled", "thrilling", "tranquil", "tranquility", 
"treat", "unreal", "vivacious", "vivid", "warm", "welcoming", "well-spoken", "win", 
"wonderful", "wonderfully", "wow", "wowed", "wowing", "wows", "yummy"
])

negative_words = set([
"a-hole", "a-holes", "abandoned", "abandoning", "abuse", "abused", "abysmal", "aggressive", 
"agonizing", "agonizingly", "agony", "ahole", "aholes", "alarming", "anger", "angering", 
"angry", "appalled", "appalling", "appalls", "argue", "argued", "arguing", "ashamed", 
"asinine", "asshole", "assholes", "atrocious", "awful", "awkward", "bad", "badgered", 
"badgering", "banal", "bankrupt", "barbaric", "bastard", "bastards", "belittled", "belligerent", 
"berated", "bigot", "bigoted", "bigots", "bitch", "bland", "bonkers", "boring", 
"bossed-around", "bothered", "bothering", "bothers", "broke", "broken", "broken-hearted", "brokenhearted", 
"brutal", "buggy", "bummed", "calamitous", "callous", "cheated", "cheating", "claustrophobic", 
"clumsy", "colorless", "colourless", "conceited", "condescending", "confused", "confuses", "confusing", 
"contentious", "corrupt", "coward", "cowardly", "cowards", "creeper", "crestfallen", "cringe-worthy", 
"cringeworthy", "cruel", "cunt", "cunts", "cursed", "cynical", "d-bag", "d-bags", 
"dbag", "dbags", "deal-breaker", "deal-breaking", "degrading", "dehumanized", "dehumanizing", "delay", 
"delayed", "deplorable", "depressed", "despicable", "destroyed", "destroying", "destroys", "detestable", 
"dick", "dicks", "died", "dirty", "disappointed", "disappointing", "disappoints", "disaster", 
"disastrous", "disastrously", "disgruntled", "disgusted", "disgusting", "disgustingly", "dismal", "disorganized", 
"disrespectful", "douche", "douchebag", "douchebags", "dour", "dreadful", "dull", "dumb", 
"egocentric", "egotistical", "embarrassing", "enraging", "erred", "erring", "error", "excruciating", 
"fail", "failed", "failing", "fails", "failure", "fake", "falsehood", "flaw", 
"flawed", "flaws", "folly", "fool", "foolish", "fools", "forgettable", "fought", 
"freaked", "freaking", "frustrated", "frustrating", "fubar", "fuck", "fuckers", "fugly", 
"furious", "gaudy", "ghastly", "gloomy", "greed", "greedy", "grief", "grieve", 
"grieved", "grieving", "grouchy", "hassle", "hate", "hated", "hating", "heart-breaking", 
"heart-broken", "heartbreaking", "heartbroken", "hellish", "hellishly", "helpless", "horrendous", "horrible", 
"horribly", "horrific", "horrifically", "humiliated", "humiliating", "hurt", "hurts", "icky", 
"idiot", "idiotic", "ignorant", "ignored", "ill", "immature", "inane", "inattentive", 
"incompetent", "incompetently", "incomplete", "inconsiderate", "incorrect", "indoctrinated", "inelegant", "infuriating", 
"infuriatingly", "insecure", "insignificant", "insufficient", "insult", "insulted", "insulting", "interrupted", 
"jaded", "kill", "lame", "loathsome", "lonely", "lose", "loser", "lost", 
"mad", "mean", "mediocre", "melodramatic", "miserable", "miserably", "misery", "missing", 
"mistake", "mistreated", "moron", "moronic", "mother-fucker", "mother-fuckers", "motherfucker", "motherfuckers", 
"mourn", "mourned", "mugged", "nagging", "nasty", "nazi", "nazis", "negative", 
"neurotic", "nonsense", "noo", "nooo", "nooooo", "nut-job", "nut-jobs", "nutjob", 
"nutjobs", "objectification", "objectified", "objectifying", "obscene", "odious", "offended", "oppressive", 
"over-sensitive", "pain", "painfully", "panic", "panicked", "panicking", "paranoid", "pathetic", 
"pessimistic", "pestered", "pestering", "petty", "pissed", "poor", "poorly", "powerless", 
"prejudiced", "pretentious", "psychopath", "psychopathic", "psychopaths", "psychotic", "quarrelling", "quarrelsome", 
"racist", "rage", "repugnant", "repulsive", "resent", "resentful", "resenting", "retarded", 
"revolting", "ridicule", "ridiculed", "ridicules", "robbed", "rude", "sad", "sadistic", 
"sadness", "scared", "screwed", "self-centered", "selfcentered", "selfish", "shambolic", "shameful", 
"shamefully", "shattered", "shit", "shitty", "shoddy", "sickening", "sloppily", "sloppy", 
"slow", "slowly", "smothered", "snafu", "spiteful", "square", "squares", "stereotyped", 
"stifled", "stressed", "stressful", "stressing", "stuck", "stuffy", "stupid", "sub-par", 
"subpar", "substandard", "suck", "sucks", "suffer", "suffering", "suicide", "superficial", 
"terrible", "terribly", "train-wreck", "trainwreck", "ugly", "unappealing", "unattractive", "uncomfortable", 
"uncomfy", "unengaging", "unengagingly", "unenticing", "unenticingly", "unexceptionable", "unfair", "unfashionable", 
"unfashionably", "unfriendly", "ungraceful", "ungrateful", "unhelpful", "unimpressive", "uninspired", "unjust", 
"unlucky", "unnotable", "unpleasant", "unpleasantly", "unsatisfactory", "unsatisfied", "unseemly", "unwelcoming", 
"upset", "vicious", "vindictive", "weak", "wreck", "wrecked", "wrecking", "wrecks", 
"wtf", "yucky"
])

intensifier_words = set([
"absolutely", "amazingly", "exceptionally", "fantastically", "fucking", "incredibly", "obscenely", "phenomenally", 
"profoundly", "really", "remarkably", "ridiculously", "so", "spectacularly", "stunningly", "such", 
"totally", "unquestionably", "very"
])

negation_words = set([
"didn't", "don't", "lack", "lacked", "no-one", "nobody", "noone", "not", "wasn't", 
])

# Returns whether a word is the positive_words / negative_words sets defined in this library
# Pig 0.9.2 does not have a boolean datatype (this is implemented in Pig 0.10+), so we use 1 = true, 0 = false. 
@outputSchema("in_word_set: int")
@null_if_input_null
def in_word_set(word, set_name):
    if set_name == 'positive':
        return (1 if word in positive_words else 0);
    elif set_name == 'negative':
        return (1 if word in negative_words else 0);
    else:
        raise ValueError('Invalid set name. Should be "positive" or "negative".')

# Estimates whether an ordered bag of words expresses a positive (> 0) or negative (< 0) sentiment.
# Accounts for intensifier words (ex. "very") and negations (ex. "not"), but only if they
# directly precede a word expressing positive/negative sentiment
# (chains, ex. intensifier -> negation -> positive-word are handled)
@outputSchema("sentiment: double")
@null_if_input_null
def sentiment(words_bag):
    if len(words_bag) == 0:
        return 0.0

    score = 0.0

    words = [t[0] for t in words_bag if len(t) > 0]
    positive = [i for i, word in enumerate(words) if word in positive_words]
    negative = [i for i, word in enumerate(words) if word in negative_words]

    for idx in positive:
        word_score = 1.0
        num_negations = 0

        i = idx - 1
        while i >= 0:
            if words[i] in intensifier_words:
                word_score += 1
            elif words[i] in negation_words:
                num_negations += 1
            else:
                break
            i -= 1

        score += word_score * ((-0.5 ** num_negations) if num_negations > 0 else 1)

    for idx in negative:
        word_score = -1.0
        num_negations = 0

        i = idx - 1
        while i >= 0:
            if words[i] in intensifier_words:
                word_score += 1
            elif words[i] in negation_words:
                num_negations += 1
            else:
                break
            i -= 1

        score += word_score * ((-0.5 ** num_negations) if num_negations > 0 else 1)

    return score

