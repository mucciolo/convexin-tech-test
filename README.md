# Convexin Tech Test

Usage:
```
sbt run <input-path> <output-path> ?<aws-profile>
```

Paths can be from local filesystem or use Hadoop S3A protocol.

S3 bucket example:
```
sbt run s3a://bucket/* s3a://bucket/output aws-profile
```