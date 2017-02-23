from datetime import datetime
from analyzers import Analyzer


class AnalyzeCoverageModes(Analyzer):
    @staticmethod
    def get_modes(stat_dict):
        journeys = stat_dict.get("journeys", None)
        if not journeys or not len(journeys):
            return
        coverages = stat_dict.get("coverages", None)
        if not coverages or not len(coverages):
            return
        for journey in journeys:
            journeys_local = []
            sections = journey.get("sections", None)
            if not sections or not len(sections):
                continue
            for section in sections:
                type_and_mode = {
                    "type": section.get("type"),
                    "mode": section.get("mode", ""),
                    "commercial_mode_id": section.get("commercial_mode_id", ""),
                    "commercial_mode_name": section.get("commercial_mode_name", ""),
                }
                if type_and_mode not in journeys_local:
                    journeys_local.append(type_and_mode)
                    yield type_and_mode

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        for type_and_mode in AnalyzeCoverageModes.get_modes(stat_dict):
            result.append(
                (
                    (
                        (
                            stat_dict['coverages'][0]['region_id'],
                            type_and_mode.get("type"),
                            type_and_mode.get("mode", ""),
                            type_and_mode.get("commercial_mode_id", ""),
                            type_and_mode.get("commercial_mode_name", ""),
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
            coverage_modes = rdd.flatMap(
                self.get_tuples_from_stat_dict
            ).reduceByKey(
                lambda (a), (b): (a + b)
            ).collect()
            return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in coverage_modes]
        else:
            return []

    def get_data(self):
        files = self.get_files_to_analyze()
        rdd = self.load_data(files, rdd_mode=True)
        return self.collect_data_from_df(rdd)

    def truncate_and_insert(self, data):
        if len(data):
            columns = ('region_id', 'type', 'mode', 'commercial_mode_id', 'commercial_mode_name',
                       'is_internal_call', "request_date", 'nb')
            self.database.insert(table_name="coverage_modes", columns=columns, data=data,
                                 start_date=self.start_date,
                                 end_date=self.end_date)

    def launch(self):
        coverage_modes = self.get_data()
        self.truncate_and_insert(coverage_modes)

    @property
    def analyzer_name(self):
        return "CoverageModes"
