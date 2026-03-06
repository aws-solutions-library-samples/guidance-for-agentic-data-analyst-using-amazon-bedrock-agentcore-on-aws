#!/bin/bash
set -e

SKIP_CDK=false
if [[ "$1" == "--skip-cdk" ]]; then
    SKIP_CDK=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infrastructure"
UI_DIR="$SCRIPT_DIR/../user-interface"

if [[ "$SKIP_CDK" == false ]]; then
    cd "$INFRA_DIR"
    echo "=== Deploying CDK stacks ==="
    cdk deploy --all --require-approval never
else
    echo "=== Skipping CDK deployment ==="
fi

cd "$SCRIPT_DIR/.."

STACK_NAME="WebAppStack"
get_output() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

BUCKET_NAME=$(get_output "UIBucketName")
DISTRIBUTION_ID=$(get_output "DistributionId")
COGNITO_USER_POOL_ID=$(get_output "CognitoUserPoolId")
COGNITO_CLIENT_ID=$(get_output "CognitoClientId")
COGNITO_IDENTITY_POOL_ID=$(get_output "CognitoIdentityPoolId")
AGENT_RUNTIME_ARN=$(get_output "AgentRuntimeArn")
AWS_REGION=$(echo "$AGENT_RUNTIME_ARN" | cut -d: -f4)

echo "=== Generating .env file ==="
cat > "$UI_DIR/.env" << EOF
REACT_APP_COGNITO_USER_POOL_ID=$COGNITO_USER_POOL_ID
REACT_APP_COGNITO_CLIENT_ID=$COGNITO_CLIENT_ID
REACT_APP_COGNITO_IDENTITY_POOL_ID=$COGNITO_IDENTITY_POOL_ID
REACT_APP_AGENT_RUNTIME_ARN=$AGENT_RUNTIME_ARN
REACT_APP_AWS_REGION=$AWS_REGION
EOF

echo "=== Building React UI ==="
cd "$UI_DIR"
npm install
npm run build

echo "=== Deploying UI to S3 ==="
aws s3 sync build/ "s3://$BUCKET_NAME" --delete

echo "=== Invalidating CloudFront cache ==="
aws cloudfront create-invalidation --distribution-id "$DISTRIBUTION_ID" --paths "/*"

echo "=== Deployment complete ==="
WEBAPP_URL=$(get_output "WebAppURL")
echo "Web App URL: $WEBAPP_URL"
echo "User Pool ID: $COGNITO_USER_POOL_ID"
