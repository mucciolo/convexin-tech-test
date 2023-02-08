# Convexin Tech Test

Usage:
```
scala ConvexinTechTest.scala <input-path> <output-path> ?<aws-profile>
```

Paths can be from local filesystem or use Hadoop S3A protocol.

S3 bucket example:
```
scala ConvexinTechTest.scala s3a://bucket/* s3a://bucket/output aws-profile
```