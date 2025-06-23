from airflow.models.variable import Variable
from airflow.timetables.interval import CronDataIntervalTimetable


class CustomTimetable(CronDataIntervalTimetable):
    def __init__(self, *args, something=Variable.get("something"), **kwargs):
        self._something = something
        super().__init__(*args, **kwargs)
