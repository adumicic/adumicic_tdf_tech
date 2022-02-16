AWS_SECRET_KEY_NAME=tdf_test/api_key
aws secretsmanager create-secret --name $AWS_SECRET_KEY_NAME --secret-string 20c80020f4e04898a6342122211706