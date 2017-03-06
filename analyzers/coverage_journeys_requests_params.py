from analyzers.analyzer import Analyzer
from datetime import datetime


class AnalyzeCoverageJourneysRequestsParams(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        parameters = stat_dict.get('parameters', [])
        if not len(parameters):
            return []
        # only keeps requests having wheelchair == true param
        if not any(param['key'] == 'wheelchair' and param['value'].lower() == 'true' for param in parameters):
            return []

        result.append((
            datetime.utcfromtimestamp(stat_dict['request_date']).date(),
            stat_dict['coverages'][0]['region_id'],
            1 if 'canaltp' in stat_dict['user_name'] else 0  # is_internal_call
        ))
        # builds (tuple, count) pairs to allow counting
        return map(lambda s: (s, 1), result)

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
        coverage_journeys_requests_params = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journeys_requests_params)

    @property
    def analyzer_name(self):
        return "CoverageJourneysRequestsParams"
