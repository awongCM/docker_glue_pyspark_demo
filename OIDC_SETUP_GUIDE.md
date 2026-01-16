# AWS OIDC Setup Guide for GitHub Actions

This guide explains how to set up OpenID Connect (OIDC) authentication between GitHub Actions and AWS for secure, keyless deployments.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [AWS Setup](#aws-setup)
- [GitHub Configuration](#github-configuration)
- [Testing the Setup](#testing-the-setup)
- [Troubleshooting](#troubleshooting)

## Overview

OIDC allows GitHub Actions to authenticate with AWS without storing long-lived AWS credentials. Instead, GitHub generates short-lived tokens that AWS validates and exchanges for temporary credentials.

**Benefits:**
- ✅ No long-lived AWS access keys in GitHub Secrets
- ✅ Automatic credential rotation
- ✅ Fine-grained access control per repository/branch
- ✅ Improved security posture
- ✅ AWS CloudTrail audit logging

## Prerequisites

- AWS Account with admin access
- GitHub repository with Actions enabled
- AWS CLI installed locally
- Terraform (optional, for infrastructure management)

## AWS Setup

### Step 1: Create OIDC Identity Provider

#### Option A: Using AWS Console

1. Navigate to **IAM Console** → **Identity providers** → **Add provider**
2. Configure the provider:
   - **Provider type:** OpenID Connect
   - **Provider URL:** `https://token.actions.githubusercontent.com`
   - **Audience:** `sts.amazonaws.com`
3. Click **Get thumbprint** (should be `6938fd4d98bab03faadb97b34396831e3780aea1`)
4. Click **Add provider**
5. Copy the provider ARN (e.g., `arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com`)

#### Option B: Using AWS CLI

```bash
aws iam create-open-id-connect-provider \
  --url "https://token.actions.githubusercontent.com" \
  --thumbprint-list "6938fd4d98bab03faadb97b34396831e3780aea1" \
  --client-id-list "sts.amazonaws.com"
```

#### Option C: Using Terraform

```hcl
resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = [
    "sts.amazonaws.com",
  ]

  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1"
  ]

  tags = {
    Name        = "github-actions-oidc"
    ManagedBy   = "terraform"
  }
}
```

### Step 2: Create IAM Role with Trust Policy

#### Trust Policy Template

Create a file `github-actions-trust-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:*"
        }
      }
    }
  ]
}
```

**Trust Policy Options:**

```json
// Restrict to specific branch
"token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:ref:refs/heads/main"

// Restrict to specific environment
"token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:environment:production"

// Allow any branch (use with caution)
"token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:*"

// Allow pull requests
"token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:pull_request"
```

#### Create the IAM Role

```bash
# Replace placeholders with your values
export AWS_ACCOUNT_ID="123456789012"
export GITHUB_ORG="your-org"
export GITHUB_REPO="docker_glue_pyspark_demo"

# Update the trust policy file
sed -i "s/YOUR_ACCOUNT_ID/$AWS_ACCOUNT_ID/g" github-actions-trust-policy.json
sed -i "s/YOUR_ORG/$GITHUB_ORG/g" github-actions-trust-policy.json
sed -i "s/YOUR_REPO/$GITHUB_REPO/g" github-actions-trust-policy.json

# Create the role
aws iam create-role \
  --role-name GitHubActionsDeployRole \
  --assume-role-policy-document file://github-actions-trust-policy.json \
  --description "Role for GitHub Actions to deploy to AWS" \
  --tags Key=ManagedBy,Value=github-actions Key=Repository,Value=$GITHUB_REPO
```

### Step 3: Attach Permissions to the Role

Create a permissions policy file `github-actions-permissions-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-ARTIFACTS-BUCKET/*",
        "arn:aws:s3:::YOUR-ARTIFACTS-BUCKET",
        "arn:aws:s3:::YOUR-GLUE-SCRIPTS-BUCKET/*",
        "arn:aws:s3:::YOUR-GLUE-SCRIPTS-BUCKET",
        "arn:aws:s3:::bronze-bucket/*",
        "arn:aws:s3:::iceberg/*"
      ]
    },
    {
      "Sid": "GlueJobManagement",
      "Effect": "Allow",
      "Action": [
        "glue:CreateJob",
        "glue:UpdateJob",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      "Resource": [
        "arn:aws:glue:*:*:job/*"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/gold_table_*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": [
        "arn:aws:logs:*:*:log-group:/aws/glue/*",
        "arn:aws:logs:*:*:log-group:pyspark-logs*"
      ]
    },
    {
      "Sid": "PassRoleToGlue",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::*:role/GlueExecutionRole",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "glue.amazonaws.com"
        }
      }
    },
    {
      "Sid": "STSGetCallerIdentity",
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}
```

Attach the policy:

```bash
# Create inline policy
aws iam put-role-policy \
  --role-name GitHubActionsDeployRole \
  --policy-name GitHubActionsDeployPolicy \
  --policy-document file://github-actions-permissions-policy.json

# Or create and attach a managed policy
aws iam create-policy \
  --policy-name GitHubActionsDeployPolicy \
  --policy-document file://github-actions-permissions-policy.json

aws iam attach-role-policy \
  --role-name GitHubActionsDeployRole \
  --policy-arn arn:aws:iam::$AWS_ACCOUNT_ID:policy/GitHubActionsDeployPolicy
```

### Step 4: Create Glue Execution Role

Glue jobs need their own execution role:

```bash
# Trust policy for Glue
cat > glue-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create the role
aws iam create-role \
  --role-name GlueExecutionRole \
  --assume-role-policy-document file://glue-trust-policy.json

# Attach AWS managed policy
aws iam attach-role-policy \
  --role-name GlueExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create custom policy for your resources
cat > glue-permissions-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::bronze-bucket/*",
        "arn:aws:s3:::iceberg/*",
        "arn:aws:s3:::YOUR-GLUE-SCRIPTS-BUCKET/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/gold_table_*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name GlueExecutionRole \
  --policy-name GlueCustomPermissions \
  --policy-document file://glue-permissions-policy.json
```

## GitHub Configuration

### Step 1: Add Secrets to GitHub Repository

Navigate to your repository: **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add the following secrets:

| Secret Name | Value | Description |
|------------|-------|-------------|
| `AWS_ROLE_ARN` | `arn:aws:iam::123456789012:role/GitHubActionsDeployRole` | IAM role ARN for OIDC authentication |
| `AWS_ACCOUNT_ID` | `123456789012` | Your AWS account ID |
| `ARTIFACTS_BUCKET` | `your-artifacts-bucket` | S3 bucket for Poetry packages |
| `GLUE_SCRIPTS_BUCKET` | `your-glue-scripts-bucket` | S3 bucket for Glue job scripts |
| `GLUE_EXECUTION_ROLE_ARN` | `arn:aws:iam::123456789012:role/GlueExecutionRole` | IAM role for Glue jobs |
| `TERRAFORM_STATE_BUCKET` | `your-terraform-state-bucket` | S3 bucket for Terraform state (optional) |

### Step 2: Workflow Configuration

The workflow is already configured in `.github/workflows/deploy-aws.yml`. Key sections:

```yaml
permissions:
  id-token: write   # Required for OIDC
  contents: read    # Required for checkout

jobs:
  deploy:
    steps:
      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          role-session-name: GitHubActions-Deploy
          aws-region: us-east-1
```

## Testing the Setup

### 1. Test OIDC Authentication

Create a simple test workflow:

```yaml
name: Test OIDC

on: workflow_dispatch

permissions:
  id-token: write
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          role-session-name: GitHubActions-Test
          aws-region: us-east-1
      
      - name: Verify Identity
        run: |
          aws sts get-caller-identity
          echo "✅ OIDC authentication successful!"
```

### 2. Verify S3 Access

```bash
# In your workflow
- name: Test S3 Access
  run: |
    echo "test" > test.txt
    aws s3 cp test.txt s3://${{ secrets.ARTIFACTS_BUCKET }}/test/
    aws s3 rm s3://${{ secrets.ARTIFACTS_BUCKET }}/test/test.txt
    echo "✅ S3 access verified!"
```

### 3. Test Glue Job Creation

```bash
# Create a simple test job
aws glue create-job \
  --name test-oidc-job \
  --role ${{ secrets.GLUE_EXECUTION_ROLE_ARN }} \
  --command Name=glueetl,ScriptLocation=s3://your-bucket/test.py
```

## Troubleshooting

### Error: "Not authorized to perform sts:AssumeRoleWithWebIdentity"

**Cause:** Trust policy doesn't match your repository/branch

**Solution:** Verify the trust policy condition:
```bash
aws iam get-role --role-name GitHubActionsDeployRole --query 'Role.AssumeRolePolicyDocument'
```

Check that `token.actions.githubusercontent.com:sub` matches your repository pattern.

### Error: "No OpenIDConnect provider found"

**Cause:** OIDC provider not created or incorrect URL

**Solution:**
```bash
# List OIDC providers
aws iam list-open-id-connect-providers

# Verify the provider exists with correct URL
aws iam get-open-id-connect-provider \
  --open-id-connect-provider-arn arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com
```

### Error: "Access Denied" when uploading to S3

**Cause:** IAM role lacks S3 permissions

**Solution:** Add S3 permissions to the role policy:
```bash
aws iam get-role-policy \
  --role-name GitHubActionsDeployRole \
  --policy-name GitHubActionsDeployPolicy
```

### Error: "User is not authorized to perform iam:PassRole"

**Cause:** Missing PassRole permission for Glue execution role

**Solution:** Ensure the role policy includes:
```json
{
  "Effect": "Allow",
  "Action": "iam:PassRole",
  "Resource": "arn:aws:iam::*:role/GlueExecutionRole",
  "Condition": {
    "StringEquals": {
      "iam:PassedToService": "glue.amazonaws.com"
    }
  }
}
```

### Debug Mode

Enable debug logging in GitHub Actions:

1. Go to **Settings** → **Secrets and variables** → **Actions** → **Variables**
2. Add variable `ACTIONS_STEP_DEBUG` with value `true`
3. Add variable `ACTIONS_RUNNER_DEBUG` with value `true`

### CloudTrail Monitoring

Monitor OIDC authentication in CloudTrail:

```bash
# View recent AssumeRoleWithWebIdentity events
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=EventName,AttributeValue=AssumeRoleWithWebIdentity \
  --max-results 10
```

## Security Best Practices

1. **Use Least Privilege:** Only grant permissions required for the deployment
2. **Restrict by Branch:** Limit production deployments to `main` branch only
3. **Use Environments:** Configure GitHub environments with protection rules
4. **Enable CloudTrail:** Monitor all AWS API calls from GitHub Actions
5. **Rotate Regularly:** Review and update IAM policies quarterly
6. **Use Tags:** Tag all resources created by GitHub Actions for tracking
7. **Set Session Duration:** Configure maximum session duration in trust policy
8. **Monitor Failures:** Set up CloudWatch alarms for failed authentications

## Additional Resources

- [GitHub Docs: Configuring OIDC in AWS](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services)
- [AWS Blog: Use IAM roles to connect GitHub Actions to AWS](https://aws.amazon.com/blogs/security/use-iam-roles-to-connect-github-actions-to-actions-in-aws/)
- [AWS IAM OIDC Provider Documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_oidc.html)
- [Terraform AWS Provider: Assuming IAM Role with Web Identity](https://registry.terraform.io/providers/hashicorp/aws/latest/docs#assuming-an-iam-role-using-a-web-identity)

## Support

For issues specific to this project, please open an issue in the repository.

For AWS OIDC setup issues, consult:
- AWS Support Console
- AWS re:Post Community
- GitHub Community Discussions
