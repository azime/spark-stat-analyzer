#!/bin/bash

pushd migrations

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

popd

echo 'Run analyzer: '${TABLE}

YESTERDAY_DATE=`date --date="yesterday" +%Y-%m-%d`
START_DATE=${START_DATE:-$YESTERDAY_DATE}
END_DATE=${END_DATE:-$YESTERDAY_DATE}

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
