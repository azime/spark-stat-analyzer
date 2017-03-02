from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date
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
                    request_date(stat_dict),
                    region_id(stat_dict),
                    is_internal_call(stat_dict)
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
        journeys_stats = self.get_data(rdd_mode=True)
        self.truncate_and_insert(journeys_stats)

    @property
    def analyzer_name(self):
        return "CoverageJourneys"
