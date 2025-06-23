from airflow.models.variable import Variable
from airflow.timetables.interval import CronDataIntervalTimetable


class CustomTimetable(CronDataIntervalTimetable):
    def __init__(self, *args, something="something", **kwargs):
        self._something = Variable.get(something)
        super().__init__(*args, **kwargs)
