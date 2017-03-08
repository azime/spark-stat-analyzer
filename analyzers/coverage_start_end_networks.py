from analyzers import Analyzer
from analyzers.stat_utils import is_internal_call, region_id, request_date


class AnalyzeCoverageStartEndNetworks(Analyzer):

    @staticmethod
    def get_networks(stat_dict):
        coverages = stat_dict.get("coverages", [])
        if not len(coverages):
            return
        journeys = stat_dict.get("journeys", [])
        if not len(journeys):
            return
        for journey in journeys:
            sections = journey.get("sections", [])
            section_public_transport = [section for section in sections if section.get("network_id", '').strip()]

            if not len(section_public_transport):
                continue

            start_network = section_public_transport[0]
            end_network = section_public_transport[len(section_public_transport) - 1]

            yield {
                "start_network_id": start_network.get("network_id"),
                "start_network_name": start_network.get("network_name", ''),
                "end_network_id": end_network.get("network_id"),
                "end_network_name": end_network.get("network_name", '')
            }

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        return map(
            lambda network:
            (
                (
                    region_id(stat_dict),
                    network.get("start_network_id"),
                    network.get("start_network_name"),
                    network.get("end_network_id"),
                    network.get("end_network_name"),
                    is_internal_call(stat_dict),
                    request_date(stat_dict)
                ),
                1
            ),
            AnalyzeCoverageStartEndNetworks.get_networks(stat_dict)
        )

    def truncate_and_insert(self, data):
        columns = ('region_id', 'start_network_id', 'start_network_name',
                   'end_network_id', 'end_network_name', 'is_internal_call', "request_date", 'nb')
        self.database.insert(table_name="coverage_start_end_networks", columns=columns, data=data,
                             start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        networks = self.get_data(rdd_mode=True)
        self.truncate_and_insert(networks)

    @property
    def analyzer_name(self):
        return "AnalyzeCoverageStartEndNetworks"
