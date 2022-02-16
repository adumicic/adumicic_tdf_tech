"""
Architecture diagram built with diagrams package: https://diagrams.mingrammer.com/docs/guides/edge
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.management import Cloudwatch
from diagrams.aws.integration import StepFunctions, SNS


with Diagram('TDF Arch Diagram', show=False):
    cw = Cloudwatch('Cloudwatch Events\nCron Schedule') 
    
    with Cluster('Step Function'):
        sfn = StepFunctions('Step Function')
        raw = Lambda('raw')
        curated = Lambda('curated')
        sns = SNS('Failure Email')

        with Cluster('S3'):
            raw_bucket = S3('Raw')
            curated_bucket = S3('Curated')

        raw >> sns
        raw >> raw_bucket
        curated >> sns
        curated >> curated_bucket
    
    cw >> Edge(label="Hourly", style="dashed") >> sfn >> raw >> curated 