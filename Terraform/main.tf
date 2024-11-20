#Creating a bucket for the terraform state file

resource "aws_s3_bucket" "zealous_state_bucket" {
  bucket = "ztf-state-bucket-omot-2024"

  tags = {
    Name        = "my-zstate-bucket"
    Environment = "Dev"
  }
}

#Creating a bucket for the raw data

resource "aws_s3_bucket" "zealous_bucket" {
  bucket = "zainycap-bucket"

  tags = {
    Name        = "my-capdata-bucket"
    Environment = "Dev"
  }
}

#Creating Policy for the AccessSecretManager

resource "aws_iam_policy" "access_secrets_policy" {
  name        = "AccessSecretsManagerPolicy"
  description = "Policy that allows access to AWS Secrets Manager"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "*"
      }
    ]
  })
}



# Attach the policy to your existing IAM user
resource "aws_iam_group_policy_attachment" "attach_policy_to_user" {
  group  = "CoreDataEngineer"  
  policy_arn = aws_iam_policy.access_secrets_policy.arn  # ARN of the policy you created
}
