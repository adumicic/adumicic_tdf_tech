from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_stepfunctions as _aws_stepfunctions,
    aws_stepfunctions_tasks as _aws_stepfunctions_tasks,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
import aws_cdk as cdk
from aws_cdk.aws_events import Rule, Schedule
from constructs import Construct
import os

class TdfTestStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Secret manager
        acc_no = os.environ["CDK_DEFAULT_ACCOUNT"]
        region_ = os.environ["CDK_DEFAULT_REGION"]
        
        secret = secretsmanager.Secret.from_secret_attributes(self, "ImportedSecret",
          secret_partial_arn=f"arn:aws:secretsmanager:{region_}:{acc_no}:secret:tdf_test/api_key",
      )

        # Storage Class
        storage_class = s3.StorageClass.INFREQUENT_ACCESS

        # S3 Lifecycle rule
        lifecycle_rule_raw = s3.LifecycleRule(
            enabled=True,
            expired_object_delete_marker=False,
            transitions=[s3.Transition(
                storage_class=storage_class,
                transition_after=cdk.Duration.days(30),
            )]
        )

        # S3 buckets
        bucket_raw = s3.Bucket(self, 
        "tdf_bucket_raw",
         versioned=True,
         bucket_name='my-tdf-tech-test-raw',
         removal_policy=RemovalPolicy.DESTROY,
         lifecycle_rules=[lifecycle_rule_raw],
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
        lambda_policy_raw_curated = iam.PolicyStatement(effect=iam.Effect.ALLOW, resources=[f'{bucket_raw.bucket_arn}/*'], actions=['s3:GetObject'])

        # # Lambda job
        # my_lambda = _lambda.Function(
        #     self, 'TDFHandler',
        #     runtime = _lambda.Runtime.PYTHON_3_7,
        #     function_name='tdf_handler',
        #     code = _lambda.Code.from_asset('lambda'), # folder
        #     timeout = cdk.Duration.seconds(300),
        #     handler = 'api_get.handler', # file_name.handler_function
        #     environment={
        #         "raw_bucket": bucket_raw.bucket_name,
        #         "curated_bucket": bucket_curated.bucket_name
        #     },
        #     layers=[pyarrow_layer]
        # )

        # Raw Lambda job
        lambda_raw = _lambda.Function(
            self, 'TDFRawHandler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            function_name='tdf_raw_handler',
            code = _lambda.Code.from_asset('lambda'), # folder
            timeout = cdk.Duration.seconds(300),
            handler = 'raw.handler', # file_name.handler_function
            environment={
                "raw_bucket": bucket_raw.bucket_name,
            },
            layers=[pyarrow_layer]
        )

        # Curated Lambda job
        lambda_curated = _lambda.Function(
            self, 'TDFCuratedHandler',
            runtime = _lambda.Runtime.PYTHON_3_7,
            function_name='tdf_curated_handler',
            code = _lambda.Code.from_asset('lambda'), # folder
            timeout = cdk.Duration.seconds(300),
            handler = 'curation.handler', # file_name.handler_function
            environment={
                "curated_bucket": bucket_curated.bucket_name
            },
            layers=[pyarrow_layer]
        )        

        # Add policies to Lambda
        lambda_raw.add_to_role_policy(lambda_policy_s3_raw)
        lambda_curated.add_to_role_policy(lambda_policy_s3_curated)
        lambda_curated.add_to_role_policy(lambda_policy_raw_curated)
        secret.grant_read(lambda_raw)

        # Create SNS topic
        topic = sns.Topic(self, "Topic",
            display_name="TDF Topic",
            topic_name="tdfTopic",
            )
        topic_policy = sns.TopicPolicy(self, "TopicPolicy",
            topics=[topic],
        )

        topic_policy.document.add_statements(iam.PolicyStatement(
            actions=["sns:Subscribe"],
            principals=[iam.AnyPrincipal()],
            resources=[topic.topic_arn]
        ))
        
        email_address = cdk.CfnParameter(self, "emailParam")

        # If you want to add an email subscription, update and uncomment the line below
        # topic.add_subscription(subscriptions.EmailSubscription(email_address.value_as_string))
        # topic.add_subscription(subscriptions.EmailSubscription(email_address='acdumicich@swin.edu.au'))
        topic.add_subscription(subscriptions.EmailSubscription(email_address.value_as_string))

        # Step Function to run the job
        # Create Chain
        raw_job = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "Retrieve Raw",
            lambda_function=lambda_raw,
            output_path="$.Payload",
        )

        curated_job = _aws_stepfunctions_tasks.LambdaInvoke(
            self, "Curate to Parquet",
            lambda_function=lambda_curated,
            output_path="$.Payload",
        )

        publish_message = _aws_stepfunctions_tasks.SnsPublish(self, "Publish message",
            topic=topic,
            message=_aws_stepfunctions.TaskInput.from_json_path_at("$.status"),
            result_path="$.status"            
        )

        fail_job = _aws_stepfunctions.Fail(
            self, "Fail",
            cause='AWS Batch Job Failed',
            error='DescribeJob returned FAILED'
        )

        succeed_job = _aws_stepfunctions.Succeed(
            self, "Succeeded",
            comment='AWS Batch Job succeeded'
        )

        definition = raw_job.next(_aws_stepfunctions.Choice(self, 'Raw Complete?')
                                        .when(_aws_stepfunctions.Condition.string_equals('$.status', 'SUCCEEDED'),
                                            curated_job.next(_aws_stepfunctions.Choice(self, 'Job Complete?')\
                                                            .when(_aws_stepfunctions.Condition.string_equals('$.status', 'SUCCEEDED'),succeed_job)
                                                            .otherwise(publish_message)
                                                            )
                                            )
                                        .otherwise(publish_message)
                                )
                                            
                                    


        # Create state machine
        sm = _aws_stepfunctions.StateMachine(
            self, "StateMachine",
            definition=definition,
            timeout=Duration.minutes(5),
        )

        # Add Hourly cron job Cloud Watch Event for Step Function
        rule_sf = events.Rule(self, "Schedule Rule Step Function", schedule=events.Schedule.cron(minute="0") )
        rule_sf.add_target(targets.SfnStateMachine(sm))


