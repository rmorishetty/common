#!/bin/bash

set -xv

export SPARK_MAJOR_VERSION=1

file_name=$1
export zk_url=$2
app_name=$3
mail_to=$4

/usr/bin/spark-submit \
                        --master yarn \
                        --jars /usr/hdp/current/phoenix-client/phoenix-client.jar,/usr/hdp/current/phoenix-client/lib/phoenix-spark-4.7.0.2.6.1.0-129.jar \
                        --conf "spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar" \
                        --conf "spark.yarn.stagingDir=/user/${HADOOP_USER_NAME}" \
                        $file_name

if [ $? -eq 0 ]
then
    echo "[EXPORT_TO_PHOENIX] Load data from Hive to Phoenix succeeded" |mailx -s "[EXPORT_TO_PHOENIX] $app_name [OK]" $mail_to
else
    echo "[EXPORT_TO_PHOENIX] Load data from Hive to Phoenix failed" |mailx -s "[EXPORT_TO_PHOENIX] $app_name [ERROR]" $mail_to
    exit -1
fi
