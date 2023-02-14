# Spark S3 Integration

This program reads and concatenates all CSV and TSV files in an Amazon S3 directory,
process them using Spark, then outputs the result in another S3 directory.

The files are assumed to consist of two columns of integers defining key-value pairs.
Missing values are defaulted to 0. Files headers are ignored.

After reading and concatenating the files, it outputs a TSV file, without header,
containing a single instance of each pair occurring an odd number of times across all files.

### Usage:
```
sbt run input-path output-path [aws-profile-name]
```

The input and output paths can be either local file paths or S3 bucket paths using the
Hadoop S3A protocol. The AWS profile name is optional.

### S3 bucket usage example:
```
sbt run s3a://bucket/input/* s3a://bucket/output dev
```