# Convexin Tech Test

### Usage:
```
sbt run input-path output-path [aws-profile-name]
```

The input and output paths can be either local file paths or S3 bucket paths using the
Hadoop S3A protocol. The AWS profile name is optional.

### S3 bucket usage example:
```
sbt run s3a://bucket/input/* s3a://bucket/output convexin
```