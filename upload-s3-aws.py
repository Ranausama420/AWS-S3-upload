############### upload to s3 from csv #######################
import boto3
import mimetypes
import botocore
import pandas as pd
"""" These lines of code are setting up a connection to Amazon Web Services (AWS)
Simple Storage Service (S3) using the Boto3 Python library.
The first line creates a resource object for S3, which allows you 
to interact with S3 in a high-level, Pythonic way. It requires 
authentication credentials, which are provided through the 
aws_access_key_id and aws_secret_access_key parameters.
The second line creates a client object for S3, which allows 
you to interact with S3 in a low-level, API-like way.
It only requires the service_name parameter, which specifies that
it is for interacting with S3."""
s3 = boto3.resource(
        's3',
        aws_access_key_id="*****************",
        aws_secret_access_key="*************************************",
    )
s3_client = boto3.client(
    service_name='s3',
    )

df = pd.read_csv("cdncontent.csv")
num= 0
for dat in df.values.tolist():
    uid = str(dat[4])
    if path.exists("G://finalcontent//"+uid+".mp4"):
        num=num+1
        pathfile = "PostProducedVideos/" + uid + ".mp4"
        destfile = 'G://finalcontent//' + uid + ".mp4"
        try:
            s3.Object('cdnautomatedvideosbucket', pathfile).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":

                print("# The object does not exist.")
                print("uploading....." + str(uid))
                print(num)
                mimetype, _ = mimetypes.guess_type(destfile)
                print(mimetype)
                s3_client.upload_file(destfile, 'cdnautomatedvideosbucket', pathfile,
                                ExtraArgs={'ContentType': mimetype, 'ACL': 'public-read'})

                print("done")
            else:
                print("# Something else has gone wrong.")
                raise
        else:
            print("# The object does exist.")
            print(num)
