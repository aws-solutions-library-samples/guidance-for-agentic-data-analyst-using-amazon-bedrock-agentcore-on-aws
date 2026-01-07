import os

class Config:
    # ID of Secrets Manager containing cognito parameters
    # When you delete a secret, you cannot create another one immediately
    # with the same name. Change this value if you destroy your stack and need
    # to recreate it with the same STACK_NAME.
    SECRETS_MANAGER_ID = os.environ.get("SECRETS_MANAGER_ID", "ONSParamCognitoSecret_1")

    # AWS region in which you want to deploy the cdk stack
    DEPLOYMENT_REGION = os.environ.get("DEPLOYMENT_REGION", "us-east-1")

    AGENT_ARN = os.environ.get("AGENT_ARN", "arn:aws:bedrock-agentcore:us-east-1:445875720550:runtime/ons_agent_service-m9t354Bfgq")
 