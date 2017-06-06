import boto3

"""
    This is a sample script created by Dustin T. and is not officially supported.
    He is not responsible for any issues stemming from the script and this should be ran at your own
    risk. At the time of this writing it did work on my test bucket. Please pay attention to the areas
    marked with #'s as these are comments and require your input. This script was ran with root credentials
    however you can supply your credentials in the blocks below.
"""

BUCKETNAME = ""                                             #Insert your bucket name here.
ACCESS_KEY = ""                                             #Insert your access key here.
SECRET_KEY = ""                                             #Insert your secret key here.
KEYS = []
CONTINUATION = ""
NOT_ALLOWED = [":", "<", ">", "\"", "\\", "|", "?", "*"]    #Characters not allowed in a file-name in windows.
NEW_NAME = ""
OLD_NAME = ""



s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


def check_for_bad(keys):
    for key in keys:
        for banned in NOT_ALLOWED:
            if banned in key:
                OLD_NAME = key
                NEW_NAME = key.replace(banned, '')          #The set of ''s can be changed to a specific value you'd like to replace instead of
                                                            #the banned characters, an example being '-' could be given to give a - instead of :.
                print(OLD_NAME)
                print(NEW_NAME)
                response = s3.copy_object(Bucket=BUCKETNAME,
                    CopySource=BUCKETNAME + "/" + OLD_NAME,
                    Key=NEW_NAME)                           #This copies the old file over to the same location with the new name.
                print(response)
                response = s3.delete_object(Bucket=BUCKETNAME,
                    Key=OLD_NAME)                           #This deletes the old object from your bucket.
                print(response)
    del KEYS[:]



def list_bucket(token=None):
    if token == None:
        response = s3.list_objects_v2(Bucket=BUCKETNAME)    #This lists your bucket, and will continue to list the bucket until it's fully listed.
    else:
        response = s3.list_objects_v2(Bucket=BUCKETNAME,
            ContinuationToken=token)                        #Picks up where that last listing left off.

    if(response["IsTruncated"]):
        CONTINUATION = response["NextContinuationToken"]
        for key in response["Contents"]:
            KEYS.append(key["Key"])
        check_for_bad(KEYS)
        list_bucket(CONTINUATION)
    else:
        for key in response["Contents"]:
            KEYS.append(key["Key"])
        check_for_bad(KEYS)


print("Beginning the process... output will be given on screen...")
list_bucket()
print("Process Finished...")
