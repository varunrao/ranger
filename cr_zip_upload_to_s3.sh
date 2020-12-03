#!/usr/bin/env bash

S3_BUCKET_LIST="test-emr-security"
PROFILE=account1
S3_KEY="datalake-security/artifacts/aws-blog-emr-knox/ranger"

#S3_BUCKET_LIST="test-emr-security"
#PROFILE=account1
#S3_KEY="datalake-security/artifacts/aws-blog-emr-ranger-v2/ranger"


=EMR6
#S3_BUCKET_LIST="test-emr-security-vbhamidi2"
#PROFILE=account3
#S3_KEY="artifacts/aws-blog-emr-ranger-beta/ranger"


for S3_BUCKET in $S3_BUCKET_LIST; do

    # Copy cloudformations
    pushd target;
    ls -lh
#    aws s3 cp ranger-2.1.0-SNAPSHOT-admin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-hive-plugin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-hdfs-plugin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-usersync.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
##    aws s3 cp ranger-2.1.0-SNAPSHOT-hbase-plugin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-kylin-plugin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
    aws s3 cp ranger-2.1.0-SNAPSHOT-prestodb-plugin*.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-plugin-spark.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-s3-plugin.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ../security-admin/contrib/solr_for_audit_setup.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
#    aws s3 cp ranger-2.1.0-SNAPSHOT-tagsync.tar.gz s3://$S3_BUCKET/$S3_KEY/ranger-2.1.0-SNAPSHOT/ --content-type 'application/java-archive'  --profile $PROFILE
    popd
done
