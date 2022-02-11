import aws_cdk as core
import aws_cdk.assertions as assertions

from tdf_test.tdf_test_stack import TdfTestStack

# example tests. To run these tests, uncomment this file along with the example
# resource in tdf_test/tdf_test_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TdfTestStack(app, "tdf-test")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
