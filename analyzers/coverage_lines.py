from analyzers import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date


class AnalyzeCoverageLines(Analyzer):
    @staticmethod
    def get_lines(stat_dict):
        for journey in stat_dict.get('journeys', []):
            lines_of_journey = []
            journey_sections = journey.get('sections', [])
            for section in journey_sections:
                if not section.get('line_id'):
                    continue

                line_dict = {
                    'type': section.get('type'),
                    'line_id': section.get('line_id'),
                    'line_code': section.get('line_code', ''),
                    'network_id': section.get('network_id', ''),
                    'network_name': section.get('network_name', ''),
                }

                if line_dict not in lines_of_journey:
                    lines_of_journey.append(line_dict)
                    yield line_dict

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        return map(
            lambda line:
            (
                (
                    region_id(stat_dict),
                    line['type'],
                    line['line_id'],
                    line['line_code'],
                    line['network_id'],
                    line['network_name'],
                    is_internal_call(stat_dict),
                    request_date(stat_dict)
                ),
                1
            ),
            AnalyzeCoverageLines.get_lines(stat_dict)
        )

    def truncate_and_insert(self, data):
        self.database.insert('coverage_lines',
                             ('region_id',
                              'type',
                              'line_id',
                              'line_code',
                              'network_id',
                              'network_name',
                              'is_internal_call',
                              'request_date',
                              'nb'),
                             data, self.start_date, self.end_date)

    def launch(self):
        coverage_lines = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_lines)

    @property
    def analyzer_name(self):
        return 'CoverageLines'
