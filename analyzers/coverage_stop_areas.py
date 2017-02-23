from analyzers import Analyzer
from datetime import datetime


class AnalyzeCoverageStopAreas(Analyzer):
    @staticmethod
    def get_stop_areas(stat_dict):
        if 'journeys' not in stat_dict or len(stat_dict['journeys']) <= 0:
            return
        for journey in stat_dict['journeys']:
            stop_areas_of_journey = []
            journey_sections = journey.get('sections', [])
            for sections in journey_sections:
                if sections['type'] != 'public_transport':
                    continue

                for from_or_to in ['from', 'to']:
                    if from_or_to + '_id' not in sections:
                        continue

                    admin_insee = sections.get(from_or_to + '_admin_insee', '')
                    deparment_code = '' if admin_insee == '' else admin_insee[:2]
                    stop_area_dict = {
                        "stop_area_id": sections[from_or_to + '_id'],
                        "stop_area_name": sections[from_or_to + '_name'],
                        "city_id": sections.get(from_or_to + '_admin_id', ''),
                        "city_name": sections.get(from_or_to + '_admin_name', ''),
                        "city_insee": admin_insee,
                        "department_code": deparment_code
                    }

                    if stop_area_dict not in stop_areas_of_journey:
                        stop_areas_of_journey.append(stop_area_dict)
                        yield stop_area_dict

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        for stop_area in AnalyzeCoverageStopAreas.get_stop_areas(stat_dict):
            result.append(
                (
                    (
                        stat_dict['coverages'][0]['region_id'],  # region_id
                        stop_area['stop_area_id'],
                        stop_area['stop_area_name'],
                        stop_area['city_id'],
                        stop_area['city_name'],
                        stop_area['city_insee'],
                        stop_area['department_code'],
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
                lambda (a), (b): (a + b)
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
            self.database.insert("coverage_stop_areas",
                                 ("region_id",
                                  "stop_area_id",
                                  "stop_area_name",
                                  "city_id",
                                  "city_name",
                                  "city_insee",
                                  "department_code",
                                  "is_internal_call",
                                  "request_date",
                                  "nb")
                                 , data, self.start_date, self.end_date)

    def launch(self):
        coverage_stop_areas = self.get_data()
        self.truncate_and_insert(coverage_stop_areas)

    @property
    def analyzer_name(self):
        return "CoverageStopAreas"
