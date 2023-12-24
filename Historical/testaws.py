import boto3

session = boto3.Session()
s3 = session.resource('s3')

my_bucket = s3.Bucket('tpcdi-benchmark-data')


files_to_retrieve = []
for my_bucket_object in my_bucket.objects.all():
    if my_bucket_object.key.startswith("Batch1/FINWIRE") and not my_bucket_object.key.endswith("csv"):
        files_to_retrieve.append(my_bucket_object.key)