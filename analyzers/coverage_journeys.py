from analyzers.analyzer import Analyzer
from datetime import datetime
import json


class AnalyzeCoverageJourneys(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        journeys = stat_dict.get('journeys', [])
        if not len(journeys):
            return []
        return [(
            (
                datetime.utcfromtimestamp(stat_dict['request_date']).date(),
                stat_dict['coverages'][0]['region_id'],
                1 if 'canaltp' in stat_dict['user_name'] else 0  # is_internal_call
            ),
            len(journeys)
        )]

    def collect_data_from_df(self, rdd):
        if rdd.count():
            journeys_stats = rdd.flatMap(self.get_tuples_from_stat_dict) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()
            return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in journeys_stats]
        return []

    def get_data(self):
        files = self.get_files_to_analyze()
        rdd = self.load_data(files, rdd_mode=True)
        return self.collect_data_from_df(rdd)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.insert(
                "coverage_journeys",
                ("request_date", "region_id", "is_internal_call", "nb"),
                data,
                self.start_date,
                self.end_date
            )

    def launch(self):
        journeys_stats = self.get_data()
        self.truncate_and_insert(journeys_stats)

    @property
    def analyzer_name(self):
        return "CoverageJourneys"
