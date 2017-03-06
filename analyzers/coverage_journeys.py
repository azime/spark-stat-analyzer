from analyzers.analyzer import Analyzer
from datetime import datetime


class AnalyzeCoverageJourneys(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        journeys = stat_dict.get('journeys', [])
        if not len(journeys):
            return []
        return [
            (
                (
                    datetime.utcfromtimestamp(stat_dict['request_date']).date(),
                    stat_dict['coverages'][0]['region_id'],
                    1 if 'canaltp' in stat_dict['user_name'] else 0 # is_internal_call
                ),
                len(journeys)
            )
        ]

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
        coverage_journeys = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journeys)

    @property
    def analyzer_name(self):
        return "CoverageJourneys"
