import sys
from datetime import datetime, timedelta
import json
import os
import shutil
from pyspark import SparkContext, SparkConf
from glob import glob


def get_journey_count_from_stat_dict(stat_dict):
    result = []
    if len(stat_dict['coverages']) > 0:
        for cov in stat_dict['coverages']:
            if 'region_id' in cov:  # The empty coverages key is represented by [{}]
                result.append(
                    (
                        (
                            datetime.utcfromtimestamp(stat_dict['request_date']).date(),  # request_date
                            cov['region_id'],  # region_id
                            1 if 'canaltp.fr' in stat_dict['user_name'] else 0,  # is_internal_call
                        ),
                        len(stat_dict['journeys']) if 'journeys' in stat_dict else 0,  # nb_journeys
                    )
                )
    return result


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("Missing arguments. Usage: " + sys.argv[0] + " <source_root> <start_date> <end_date> ")

    # treatment_day = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    # source_root = '/home/vlepot/dev/navitia-stat-logger/tmp'
    # source_root = 'gs://hdp_test'

    source_root = sys.argv[1]
    treatment_day_start = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    treatment_day_end = datetime.strptime(sys.argv[3], '%Y-%m-%d').date()

    print "Go for dates: " + treatment_day_start.strftime('%Y-%m-%d') + " -> " + treatment_day_end.strftime('%Y-%m-%d')
    print "Source root dir: " + source_root

    conf = SparkConf().setAppName("coverage_journeys_compiler")
    sc = SparkContext(conf=conf)

    statsLines = sc.emptyRDD()
    treatment_day = treatment_day_start
    while treatment_day <= treatment_day_end:
        if source_root.startswith("/") and \
                        len(glob(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')) > 0:
            statsLines = statsLines.union(sc.textFile(
                source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*.json.log*')
            )
        treatment_day += timedelta(days=1)

    dayStats = statsLines.map(
        lambda stat: json.loads(stat)
    )

    coverage_journeys = dayStats.flatMap(
        get_journey_count_from_stat_dict
    ).reduceByKey(
        lambda a, b: a+b
    ).map(
        lambda tuple_of_tuples: [str(element) for element in tuple_of_tuples[0]] + [str(tuple_of_tuples[1])]
    ).map(
        lambda line: ";".join(line)
    )

    # print requests_calls.count()

    # Store on FS

    compiled_storage_rootdir = source_root + "/compiled/" + treatment_day_start.strftime('%Y/%m/%d')
    compiled_storage_dir = compiled_storage_rootdir + "/coverage_journeys_" + treatment_day_start.strftime('%Y%m%d')

    if source_root.startswith('/'):  # In case of local file system do some preparation first
        if os.path.isdir(compiled_storage_dir):
            shutil.rmtree(compiled_storage_dir)
        else:
            if not os.path.isdir(compiled_storage_rootdir):
                os.makedirs(compiled_storage_rootdir)
    elif source_root.startswith('gs://'):  # In case of Google Cloud Storage do some preparation first
        pass

    coverage_journeys.saveAsTextFile(compiled_storage_dir)
