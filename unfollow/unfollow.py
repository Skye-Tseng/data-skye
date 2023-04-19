# cloud config set project media17-1119
# pipenv run python unfollow.py
from google.cloud import bigquery
import pymongo
import json
from bson import json_util
#https://console.cloud.google.com/bigquery?j=media17-1119:US:{job_id}
def get_job(client, job_id = "bquxjob_21f5ad6f_18798db98fa"):
    job = client.get_job(job_id)
    print(f"Query:\n{job.query}")

    results = job.result().to_dataframe()
    print(results.head())
    return results

def get_mongo(client, s_user_id, s_target_user_id): 
    db = client['17media']
    col = db["UserFollowV2"]
    doc = col.find_one({'followeeID':s_target_user_id,'followerID': s_user_id}) # mongo follow
    return doc

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
    for index, event_stream_doc in event_stream_docs.iterrows():
        result = get_mongo(mongo_client,s_user_id=event_stream_doc['s_userID'],s_target_user_id=event_stream_doc['s_targetUserID'])
        if result is None:
            obj = event_stream_doc.to_dict()
            print(obj)
            diff.append(obj)
        print(f"{index}/{num}")

    print(len(diff))
    with open("data.json", "w") as outfile:
        json.dump(diff, outfile, indent=4,default=json_util.default)
            

if __name__ == '__main__':
    main()