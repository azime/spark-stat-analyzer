#!/bin/bash

python -m pytest --junitxml junit.xml
RESULT=$?
chown $USER_ID:$USER_ID junit.xml
find -name "*.pyc" -delete
rm -rf spark-warehouse
rm -rf tests/__pycache__
exit $RESULT