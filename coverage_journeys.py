import sys
from datetime import datetime
import json
import os
import shutil
from pyspark import SparkContext, SparkConf


def get_journey_count_from_stat_dict(statDict):
    result = []
    if len(statDict['coverages']) > 0:
        for cov in statDict['coverages']:
            if 'region_id' in cov: # The empty coverages key is represented by [{}]
                result.append(
                    (
                        (
                            datetime.utcfromtimestamp(statDict['request_date']).date(),  # request_date
                            cov['region_id'],  # region_id
                            1 if 'canaltp.fr' in statDict['user_name'] else 0,  # is_internal_call
                        ),
                        len(statDict['journeys']) if 'journeys' in statDict else 0,  # nb_journeys
                    )
                )
    return result


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("Missing arguments. Usage: " + sys.argv[0] + " <date> <source_root>")

    source_root = sys.argv[1]
    treatment_day = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    #source_root = '/home/vlepot/dev/navitia-stat-logger/tmp'
    #source_root = 'gs://hdp_test'

    print "Go for date: " + treatment_day.strftime('%Y-%m-%d')
    print "Source root dir: " + source_root

    conf = SparkConf().setAppName("coverage_journeys_compiler")
    sc = SparkContext(conf=conf)

    statsLines = sc.textFile(source_root + '/' + treatment_day.strftime('%Y/%m/%d') + '/*')

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

    compiled_storage_rootdir = source_root + "/compiled/" + treatment_day.strftime('%Y/%m/%d')
    compiled_storage_dir = compiled_storage_rootdir + "/coverage_journeys_" + treatment_day.strftime('%Y%m%d')

    if source_root.startswith('/'): # In case of local file system do some preparation first
        if os.path.isdir(compiled_storage_dir):
            shutil.rmtree(compiled_storage_dir)
        else:
            if not os.path.isdir(compiled_storage_rootdir):
                os.makedirs(compiled_storage_rootdir)
    elif source_root.startswith('gs://'): # In case of Google Cloud Storage do some preparation first
        pass

    coverage_journeys.saveAsTextFile(compiled_storage_dir)
