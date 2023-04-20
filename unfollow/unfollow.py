# cloud config set project media17-1119
# pipenv run python unfollow.py
from google.cloud import bigquery
from bson import json_util
from multiprocessing.pool import ThreadPool as Pool
import pymongo
import json

count = 0 
pool_size = 5 

def get_job(client):

    query_job = client.query(
    """
    WITH
        DateRange AS (
        SELECT
            DATE('2023-04-19') AS inputDateStart,
            DATE('2023-04-19') AS inputDateEnd ),
        EventStreamingRaw AS (
        SELECT
            CAST(timestamp AS Date) AS date,
            CAST(timestamp AS DATETIME) AS date_time, --Skye
            s_userID, -- 用戶
            s_targetUserID,-- 直播主
            msg,
            ROW_NUMBER() OVER (PARTITION BY CAST(timestamp AS Date),
            s_userID,
            s_targetUserID
            ORDER BY
            timestamp DESC) row_number,
            CASE
            WHEN msg LIKE "%Unfollow%" THEN "unfollow"
            WHEN msg LIKE "%Follow%" THEN "follow"
            ELSE
            "error"
            END
            AS follow
        FROM
            `media17-1119.eventStreaming.userFollow`
        WHERE
            env = 'k8sprod' AND
            DATE(_PARTITIONTIME) BETWEEN (
            SELECT
            inputDateStart
            FROM
            DateRange)
            AND (
            SELECT
            inputDateEnd
            FROM
            DateRange) ),
        EventStreamingLatest AS (
        SELECT
            -- date,
            date_time,
            s_userID,
            s_targetUserID,
            msg,
            follow
        FROM
            EventStreamingRaw
        WHERE
            row_number = 1 ) -- 最新事件
        SELECT * FROM EventStreamingLatest WHERE follow = 'follow' ORDER BY date_time DESC
    """
    )

    results = query_job.result().to_dataframe()
    print(results.head())
    return results

def get_mongo(client, s_user_id, s_target_user_id): 
    db = client['17media']
    col = db["UserFollowV2"]
    doc = col.find_one({'followeeID':s_target_user_id,'followerID': s_user_id}) # mongo follow
    return doc

def worker(mongo_client, event_stream_doc, diff, count):
    result = get_mongo(mongo_client,s_user_id=event_stream_doc['s_userID'],s_target_user_id=event_stream_doc['s_targetUserID'])
    if result is None:
        obj = event_stream_doc.to_dict()
        print(obj)
        diff.append(obj)


def main():
    project = 'media17-1119' # Project ID inserted based on the query results selected to explore
    location = 'US'
    username = ''
    secret = ''
    client = bigquery.Client(project=project, location=location)
    mongo_client = pymongo.MongoClient("mongodb+srv://{}:{}@m17prod-user.8rrni.mongodb.net/17media".format(username,secret))
    event_stream_docs = get_job(client) # unfollow

    diff = []
    num = len(event_stream_docs)
    pool = Pool(pool_size)
    count = [1]
    for index, event_stream_doc in event_stream_docs.iterrows():
        pool.apply_async(worker, (mongo_client, event_stream_doc, diff, count))
        print(f"run: {index}/{num}")

    pool.close()
    pool.join()
    print(f"abnormal: {len(diff)}/{num}")
    with open("data.json", "w") as outfile:
        json.dump(diff, outfile, indent=4,default=json_util.default)
            

if __name__ == '__main__':
    main()