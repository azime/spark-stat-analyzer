from datetime import datetime
from analyzers.analyzer import Analyzer


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
                            (
                                coverages[0]['region_id'],
                                journey.get("nb_transfers"),
                                # is_internal_call
                                1 if 'canaltp' in stat_dict['user_name'] else 0,
                                # request_date
                                datetime.utcfromtimestamp(stat_dict['request_date']).date()
                            )
                        ),
                    (
                        1
                    )
                    )
                )
        return result

    def collect_data_from_df(self, rdd):
        if rdd.count():
            data = rdd.flatMap(
                self.get_tuples_from_stat_dict
            ).reduceByKey(
                lambda (a), (b): (a + b)
            ).collect()
            return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in data]
        else:
            return []

    def get_data(self):
        files = self.get_files_to_analyze()
        rdd = self.load_data(files, rdd_mode=True)
        return self.collect_data_from_df(rdd)

    def truncate_and_insert(self, data):
        if len(data):
            columns = ('region_id', 'nb_transfers', 'is_internal_call', "request_date", 'nb')
            self.database.insert(table_name="coverage_journeys_transfers", columns=columns, data=data,
                                 start_date=self.start_date,
                                 end_date=self.end_date)

    def launch(self):
        coverage_modes = self.get_data()
        self.truncate_and_insert(coverage_modes)

    @property
    def analyzer_name(self):
        return "CoverageJourneysTransfers"
