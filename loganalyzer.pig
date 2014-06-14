/**
 * Combines Papertrail logs and KISSmetrics data to determine which web users had their last session end with an error.
 *
 * Takes relevant data out of the message field of Papertrail and expands it to its own relation.
 * Loads data from KISSmetrics.
 * Sessionizes both KISSmetrics data and Papertrail data by user (either user_email or user_id).
 * Uses a third datasource lookup to translate between ids and emails
 * Identifies web sessions in KISSmetrics data by looking for overlap with Papertrail
 * Orders web actions by users to find the last one
 */

REGISTER s3n://mhc-software-mirror/datafu/datafu-0.0.9-SNAPSHOT.jar;
REGISTER s3n://mhc-software-mirror/joda-time/joda-time-2.1.jar;
REGISTER s3n://mhc-software-mirror/papertrail/papertrail-loader-0.2.jar;

define Sessionize datafu.pig.sessions.Sessionize('30m');
DEFINE UnixToISO   org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
define Enumerate   datafu.pig.bags.Enumerate();

--Get the Papertrail message field data for web requests
log_data = LOAD 's3://mortar-example-data/papertrail-logs/sample_papertrail_data.txt' USING com.mortardata.pig.PapertrailLoader();
mhc_web = FILTER log_data BY message matches '.*mhc_request.*';
full_web = FOREACH mhc_web GENERATE tab_flatten(message) as message;

DEFINE FromJson org.apache.pig.piggybank.evaluation.FromJsonWithSchema('AUTH_TYPE: chararray, HTTP_REFERER: chararray, pid:     chararray, SCRIPT_NAME: chararray, REQUEST_METHOD: chararray, PATH_INFO: chararray, SERVER_PROTOCOL: chararray, QUERY_STRING: chararray, CONTENT_LENGTH: chararray, HTTP_USER_AGENT: chararray, user_id: chararray, SERVER_NAME: chararray, REMOTE_ADDR: chararray, hostname: chararray, SERVER_PORT: chararray, timestamp: chararray, controller: chararray, HTTP_HOST: chararray, thread: chararray, HTTP_X_FORWARDED_FOR: chararray, action: chararray, X_FORWARDED_FOR: chararray');

message = FOREACH full_web GENERATE FLATTEN(FromJson(message));

--Load KISSmetrics events data
k_events = LOAD 's3n://mortar-example-data/papertrail-logs/sample_km_data.json' USING org.apache.pig.piggybank.storage.JsonLoader('_p: chararray, _t: long, _n: chararray, referrer: chararray, url: chararray');

-- Only look for users with an actual email address
k_events_f = FILTER k_events by underscore_p matches '.*@.*' and underscore_n is not null;
k_events_distinct_f = DISTINCT k_events_f;

-- Sessionize the web messages
small_message = FOREACH (FILTER message by timestamp is not null) {
    GENERATE timestamp, ISOToUnix(timestamp) as unix_time, user_id;
    };
log_sessions = FOREACH (GROUP small_message by user_id) {
    visits = ORDER small_message by timestamp;
    GENERATE FLATTEN(Sessionize(visits)) AS (timestamp, unix_time, user_id, sessionId);
    };

-- Sessionize the KM events
k_iso_time = FOREACH k_events_distinct_f
    GENERATE UnixToISO(underscore_t * 1000) as iso_time,
             underscore_p as user_email, underscore_n as event, underscore_t * 1000 as unix_time;
k_sessions = FOREACH (GROUP k_iso_time by user_email) {
    visits = ORDER k_iso_time by iso_time;
    GENERATE FLATTEN(Sessionize(visits)) AS (iso_time, user_email, event, unix_time, sessionId);
    };

-- For each Session, determine its min and max time
log_min_max = FOREACH (GROUP log_sessions by (sessionId, user_id)) {
    GENERATE MIN(log_sessions.unix_time) as min_web_time, MAX(log_sessions.unix_time) as max_web_time, FLATTEN(group);
    };

k= FOREACH (GROUP k_sessions by (sessionId, user_email)) {
    GENERATE MIN(k_sessions.unix_time) as min_web_time, MAX(k_sessions.unix_time) as max_web_time, FLATTEN(group);
    };

-- Load the users table in order to translate from Papertrail ids to KM user_emails
translation_data = LOAD 's3n://mortar-example-data/papertrail-logs/id_translation.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (email:chararray, id:chararray);
l = JOIN log_min_max by user_id, translation_data by id;
log_km_join = JOIN l by email, k by user_email;

-- Determine which KM Sessions are web sessions by looking for overlap with the Papertrail web sessions
web_sessions = FILTER log_km_join by ((l::log_min_max::min_web_time > k::min_web_time AND l::log_min_max::min_web_time < k::max_web_time) OR (k::min_web_time < l::log_min_max::max_web_time AND k::min_web_time > l::log_min_max::min_web_time));

valid_web_sessions = FOREACH web_sessions GENERATE k::group::sessionId;

--Get the KM sessionIds for web sessions
k_web_sessions = JOIN valid_web_sessions by sessionId, k_sessions by sessionId;

k_events_ranked = FOREACH (GROUP k_web_sessions by user_email) {
    ordered = order k_web_sessions by unix_time DESC;
    GENERATE FLATTEN(Enumerate(ordered));
    };
last_actions = FILTER k_events_ranked by i == 0;


rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/last_actions;
STORE last_actions INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/last_actions' USING PigStorage('\t');




-------------

PYTHON UDFS

--------------

@outputSchema('message:chararray')
def tab_flatten(str):
    str = str.replace('\t', ': ')
    return str[11:]



