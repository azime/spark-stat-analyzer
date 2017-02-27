import json
from analyzers import Analyzer
from datetime import datetime


class AnalyzeCoverageNetworks(Analyzer):
    @staticmethod
    def get_networks(stat_dict):
        for journey in stat_dict.get('journeys', []):
            networks_of_journey = []
            for section in journey.get('sections', []):
                if section.get('network_id', '') == '':
                    continue

                networks_dict = {
                    'network_id': section['network_id'],
                    'network_name': section.get('network_name', ''),
                }

                if networks_dict not in networks_of_journey:
                    networks_of_journey.append(networks_dict)
                    yield networks_dict

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        for network in AnalyzeCoverageNetworks.get_networks(stat_dict):
            result.append(
                (
                    (
                        stat_dict['coverages'][0]['region_id'],  # region_id
                        network['network_id'],
                        network['network_name'],
                        1 if 'canaltp' in stat_dict['user_name'] else 0,  # is_internal_call
                        datetime.utcfromtimestamp(stat_dict['request_date']).date(),  # request_date
                    ),
                    (
                        1
                    )
                )
            )

        return result

    def collect_data_from_df(self, rdd):
        if rdd.count():
            coverage_stop_areas = rdd.flatMap(
                self.get_tuples_from_stat_dict
            ).reduceByKey(
                lambda a, b: (a + b)
            ).collect()
            return [tuple(list(key_tuple) + [nb]) for (key_tuple, nb) in coverage_stop_areas]
        else:
            return []

    def get_data(self):
        files = self.get_files_to_analyze()
        rdd = self.load_data(files, rdd_mode=True)
        return self.collect_data_from_df(rdd)

    def truncate_and_insert(self, data):
        if len(data):
            self.database.insert("coverage_networks",
                                 ("region_id",
                                  "network_id",
                                  "network_name",
                                  "is_internal_call",
                                  "request_date",
                                  "nb")
                                 , data, self.start_date, self.end_date)

    def launch(self):
        coverage_stop_areas = self.get_data()
        self.truncate_and_insert(coverage_stop_areas)

    @property
    def analyzer_name(self):
        return "CoverageNetworks"
