#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UI_DIR="$SCRIPT_DIR/../user-interface"

STACK_NAME="WebAppStack"

USE_LOCAL_AGENT=false
if [ "$1" = "--local" ]; then
    USE_LOCAL_AGENT=true
fi

echo "=== Fetching configuration from CloudFormation ==="

get_output() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null || echo ""
}

COGNITO_USER_POOL_ID=$(get_output "CognitoUserPoolId")
COGNITO_CLIENT_ID=$(get_output "CognitoClientId")
COGNITO_IDENTITY_POOL_ID=$(get_output "CognitoIdentityPoolId")
AGENT_RUNTIME_ARN=$(get_output "AgentRuntimeArn")

if [ -z "$AGENT_RUNTIME_ARN" ]; then
    echo "Error: Could not retrieve stack outputs. Make sure the stack '$STACK_NAME' is deployed."
    exit 1
fi

AWS_REGION=$(echo "$AGENT_RUNTIME_ARN" | cut -d: -f4)

echo "=== Generating .env.local file ==="
cat > "$UI_DIR/.env.local" << EOF
VITE_COGNITO_USER_POOL_ID=$COGNITO_USER_POOL_ID
VITE_COGNITO_CLIENT_ID=$COGNITO_CLIENT_ID
VITE_COGNITO_IDENTITY_POOL_ID=$COGNITO_IDENTITY_POOL_ID
VITE_AGENT_RUNTIME_ARN=$AGENT_RUNTIME_ARN
VITE_AWS_REGION=$AWS_REGION
VITE_USE_LOCAL_AGENT=$USE_LOCAL_AGENT
EOF

echo "Configuration retrieved:"
echo "  Region: $AWS_REGION"
echo "  Agent Runtime ARN: $AGENT_RUNTIME_ARN"
echo ""

cd "$UI_DIR"

if [ ! -d "node_modules" ]; then
    echo "=== Installing dependencies ==="
    npm install
fi

echo "=== Starting development server ==="
npm start
