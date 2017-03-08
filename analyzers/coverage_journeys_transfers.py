from analyzers.stat_utils import region_id, is_internal_call, request_date
from analyzers import Analyzer


class AnalyzeCoverageJourneysTransfers(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        journeys = stat_dict.get("journeys", None)
        if not journeys or not len(journeys):
            return result
        coverages = stat_dict.get("coverages", None)
        if not coverages or not len(coverages):
            return result
        for journey in journeys:
            if 'nb_transfers' in journey:
                result.append(
                    (
                        (
                            region_id(stat_dict),
                            journey.get("nb_transfers"),
                            is_internal_call(stat_dict),
                            request_date(stat_dict)
                        ),
                        1
                    )
                )
        return result

    def truncate_and_insert(self, data):
        columns = ('region_id', 'nb_transfers', 'is_internal_call', "request_date", 'nb')
        self.database.insert(table_name="coverage_journeys_transfers", columns=columns, data=data,
                             start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        coverage_journeys_transfers = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journeys_transfers)

    @property
    def analyzer_name(self):
        return "CoverageJourneysTransfers"
