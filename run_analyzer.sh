#!/bin/bash

var=$(pwd)
echo 'Current directory : '$var

echo 'Change directory : '$var'/migrations'
cd $var'/migrations'

echo 'Run migration database'
PYTHONPATH=../ alembic upgrade head
RESULT=$?

if [ $RESULT -eq 0 ]
then
    echo "Migration database done"
else
    echo "Migration database failed"
    exit 1
fi

echo 'Change directory : '$var
cd $var

echo 'Run analyzer: '${TABLE}

${SPARK_BIN}spark-submit \
    --py-files spark-stat-analyzer.zip \
    --conf spark.ui.showConsoleProgress=true \
    --master $MASTER_OPTION \
    --driver-java-options '-Duser.timezone=UTC' \
    manage.py \
    -a ${TABLE} \
    -i $CONTAINER_STORE_PATH \
    -s $START_DATE \
    -e $END_DATE

RESULT=$?

if [ $RESULT -eq 0 ]
then
    echo "Analyzer done"
else
    echo "Analyzer failed"
fi

exit $RESULT