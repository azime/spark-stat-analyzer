from analyzers.analyzer import Analyzer
from datetime import datetime
import json

class AnalyzeCoverageJourneysRequestsParams(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        interpreted_parameters = stat_dict.get('interpreted_parameters', [])
        if not len(interpreted_parameters):
            return []
        # only keeps requests having wheelchair == true param
        if not any(param['key'] == 'wheelchair' and param['value'].lower() == 'true' for param in interpreted_parameters):
            return []

        result.append((
            datetime.utcfromtimestamp(stat_dict['request_date']).date(),
            stat_dict['coverages'][0]['region_id'],
            1 if 'canaltp' in stat_dict['user_name'] else 0  # is_internal_call
        ))
        # builds (tuple, count) pairs to allow counting
        return map(lambda s: (s, 1), result)

    def collect_data_from_df(self, rdd):
        if rdd.count():
            wheelchair_stats = rdd.flatMap(self.get_tuples_from_stat_dict) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()
            return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in wheelchair_stats]
        return []

    def get_data(self):
        files = self.get_files_to_analyze()
        rdd = self.load_data(files, rdd_mode=True)
        return self.collect_data_from_df(rdd)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.insert(
                "coverage_journeys_requests_params",
                ("request_date", "region_id", "is_internal_call", "nb_wheelchair"),
                data,
                self.start_date,
                self.end_date
            )

    def launch(self):
        wheelchair_stats = self.get_data()
        self.truncate_and_insert(wheelchair_stats)

    @property
    def analyzer_name(self):
        return "CoverageJourneysRequestsParams"
