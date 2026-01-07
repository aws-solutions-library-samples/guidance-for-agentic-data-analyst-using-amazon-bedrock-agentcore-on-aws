# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Amazon Software License (the "License"). You may not use
# this file except in compliance with the License. A copy of the License is
# located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.
import boto3
import json
from streamlit_cognito_auth import CognitoAuthenticator
from sys import platform


class LocalMockAuthenticator:
    def login(self):
        return True

    def logout(self):
        pass

    def get_username(self):
        return "dev"


class Auth:
    def __init__(self, secret_id, region):
        if platform == "darwin":
            self.authenticator = LocalMockAuthenticator()
        else:
            # Get Cognito parameters from Secrets Manager
            secretsmanager_client = boto3.client("secretsmanager", region_name=region)
            response = secretsmanager_client.get_secret_value(SecretId=secret_id)
            secret_string = json.loads(response['SecretString'])

            # Initialise CognitoAuthenticator
            self.authenticator = CognitoAuthenticator(
                pool_id=secret_string['pool_id'],
                app_client_id=secret_string['app_client_id'],
                app_client_secret=secret_string['app_client_secret'],
            )
