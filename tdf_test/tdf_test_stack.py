from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue,
)
import aws_cdk as cdk
from aws_cdk.aws_events import Rule, Schedule
from constructs import Construct

class TdfTestStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # s3 bucket      
        bucket_raw = s3.Bucket(self, 
        "tdf_bucket_raw",
         versioned=True,
         bucket_name='my-tdf-tech-test-raw',
         removal_policy=RemovalPolicy.DESTROY,
         auto_delete_objects=True)

        bucket_curated = s3.Bucket(self, 
        "tdf_bucket_curated",
         versioned=True,
         bucket_name='my-tdf-tech-test-curated',
         removal_policy=RemovalPolicy.DESTROY,
         auto_delete_objects=True)

        # PyArrow Layer
        pyarrow_layer = _lambda.LayerVersion(self, 'pyarrow_layer',
                                     code=_lambda.Code.from_asset("layer"),
                                     description='PyArrow Library',
                                     compatible_runtimes=[_lambda.Runtime.PYTHON_3_6, _lambda.Runtime.PYTHON_3_7, _lambda.Runtime.PYTHON_3_8],
                                     removal_policy=RemovalPolicy.DESTROY
                                     )

        # Lambda policies for S3 access
        lambda_policy_s3_raw = iam.PolicyStatement(effect=iam.Effect.ALLOW, resources=[f'{bucket_raw.bucket_arn}/*'], actions=['s3:PutObject'])
        lambda_policy_s3_curated = iam.PolicyStatement(effect=iam.Effect.ALLOW, resources=[f'{bucket_curated.bucket_arn}/*'], actions=['s3:PutObject'])
        lambda_policy_sm = iam.PolicyStatement(effect=iam.Effect.ALLOW, resources=['*'], actions=['secretsmanager:GetSecretValue'])

        # Lambda job
        my_lambda = _lambda.Function(
            self, 'TDFHandler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            function_name='tdf_handler',
            code = _lambda.Code.from_asset('lambda'), # folder
            timeout = cdk.Duration.seconds(300),
            handler = 'api_get.handler', # file_nyame.handler_function
            environment={
                "raw_bucket": bucket_raw.bucket_name,
                "curated_bucket": bucket_curated.bucket_name
            },
            layers=[pyarrow_layer]
        )

        # Add policies to Lambda
        my_lambda.add_to_role_policy(lambda_policy_s3_raw)
        my_lambda.add_to_role_policy(lambda_policy_s3_curated)
        my_lambda.add_to_role_policy(lambda_policy_sm)

        # Add Hourly cron job Cloud Watch Event
        rule = events.Rule(self, "Schedule Rule", schedule=events.Schedule.cron(minute="0") )
        rule.add_target(targets.LambdaFunction(my_lambda))

        # Step function?
            # SNS error reporting
        # Secret Manager arn
        # S3 lifecycle management

        # Add in arch diagram