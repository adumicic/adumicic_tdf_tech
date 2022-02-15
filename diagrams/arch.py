from diagrams import Diagram, Cluster
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.management import Cloudwatch


with Diagram('TDF Arch Diagram', show=False):
    cw = Cloudwatch('Hourly Schedule') 

    with Cluster('S3 Buckets'):
        s3_buckets = [S3('Raw'), S3('Curated')]
    
    cw >> Lambda('api_get') >> s3_buckets