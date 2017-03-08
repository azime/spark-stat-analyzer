from analyzers.stat_utils import region_id, is_internal_call, request_date
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
        return map(
            lambda val:
            (
                (
                    region_id(stat_dict),
                    val.get("type"),
                    val.get("mode", ""),
                    val.get("commercial_mode_id", ""),
                    val.get("commercial_mode_name", ""),
                    is_internal_call(stat_dict),
                    request_date(stat_dict)
                ),
                1
            ),
            AnalyzeCoverageModes.get_modes(stat_dict)
        )

    def truncate_and_insert(self, data):
        columns = ('region_id', 'type', 'mode', 'commercial_mode_id', 'commercial_mode_name',
                   'is_internal_call', "request_date", 'nb')
        self.database.insert(table_name="coverage_modes", columns=columns, data=data,
                             start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        coverage_modes = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_modes)

    @property
    def analyzer_name(self):
        return "CoverageModes"
