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

#echo 'Run analyzer: '${TABLE}

usr/local/spark/bin/spark-submit --conf spark.ui.showConsoleProgress=true --master='local[8]' manage.py -i /home/azime/statistics/spark-stat-analyzer/tests/fixtures/coverage_modes -a coverage_modes -s 2017-02-22 -e 2017-02-22

RESULT=$?

if [ $RESULT -eq 0 ]
then
    echo "Analyzer done"
else
    echo "Analyzer failed"
fi

exit $RESULT
