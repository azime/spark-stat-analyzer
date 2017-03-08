from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date


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
            request_date(stat_dict),
            region_id(stat_dict),
            is_internal_call(stat_dict)
        ))
        # builds (tuple, count) pairs to allow counting
        return map(lambda s: (s, 1), result)

    def truncate_and_insert(self, data):
        self.database.insert(
            "coverage_journeys_requests_params",
            ("request_date", "region_id", "is_internal_call", "nb_wheelchair"),
            data,
            self.start_date,
            self.end_date
        )

    def launch(self):
        wheelchair_stats = self.get_data(rdd_mode=True)
        self.truncate_and_insert(wheelchair_stats)

    @property
    def analyzer_name(self):
        return "CoverageJourneysRequestsParams"
