from analyzers import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date


class AnalyzeCoverageNetworks(Analyzer):
    @staticmethod
    def get_networks(stat_dict):
        for journey in stat_dict.get('journeys', []):
            networks_of_journey = []
            for section in journey.get('sections', []):
                if not section.get('network_id', None):
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
        return map(
            lambda network:
            (
                (
                    region_id(stat_dict),
                    network['network_id'],
                    network['network_name'],
                    is_internal_call(stat_dict),
                    request_date(stat_dict)
                ),
                1
            ),
            AnalyzeCoverageNetworks.get_networks(stat_dict)
        )

    def truncate_and_insert(self, data):
        self.database.insert("coverage_networks",
                             ("region_id",
                              "network_id",
                              "network_name",
                              "is_internal_call",
                              "request_date",
                              "nb"),
                             data, self.start_date, self.end_date)

    def launch(self):
        coverage_networks = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_networks)

    @property
    def analyzer_name(self):
        return "CoverageNetworks"
