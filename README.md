import boto3
from botocore.config import Config

awsAccessKeyId = dbutils.secrets.get("scopeName", key = "AWSAccessKeyID")
awsSecretAccessKey = dbutils.secrets.get("scopeName", key = "AWSSecretAccessKey")

botoConfig = Config(
        region_name = 'eu-west-2',
        signature_version = 'v4',
        retries = {
            'max_attempts': 10,
            'mode': 'standard'
        }
    )


client = boto3.client('sts', config = botoConfig, aws_access_key_id = awsAccessKeyId, aws_secret_access_key = awsSecretAccessKey)
response = client.assume_role(
        RoleArn='arn:aws:iam::1234567890:role/role_name',
        RoleSessionName='AzureDatabricks',
        DurationSeconds=3600
    )

credResponse = response['Credentials']



spark.conf.set("fs.s3a.credentialsType", "AssumeRole")
spark.conf.set("fs.s3a.stsAssumeRole.arn", "arn:aws:iam::1234567890:role/role_name")
spark.conf.set("fs.s3a.acl.default", "BucketOwnerFullControl")

sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", credResponse['AccessKeyId'] )
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credResponse['SecretAccessKey'])
sc._jsc.hadoopConfiguration().set("fs.s3a.session.token", credResponse['SessionToken'])



df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .schema(definedSchema)
      .load("s3a://bucket/directory/")
     )



(df.writeStream.format("delta") 
  .option("checkpointLocation", "/mnt/lake/directory/_checkpoint") 
  .trigger(once=True)
  .start("/mnt/lake/directory/")
)
