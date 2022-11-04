import csv
import os
import statistics
from dataclasses import asdict, dataclass

from utils import YandexWeatherAPI

CSV_FILENAME = 'data.csv'
TIME_LIST = [i for i in range(9, 20)]
GOOD_WEATHER = (
    'clear',
    'partly-cloudy',
    'cloudy',
    'overcast',
)


@dataclass
class CalculationData:
    city: str
    days: dict
    avg_tem: int
    no_precipitation_time: int
    rating: int = 0


class DataFetchingTask:
    source = YandexWeatherAPI()

    def get_weather(self, city):
        return city, self.source.get_forecasting(city)


class DataCalculationTask:

    def __init__(self, city, data):
        self.city: str = city
        self.forecasts: dict = data.get('forecasts')
        self.temperatures: dict[str, int] = {}
        self.no_precipitation_time: int = 0

    def _calculation(self):
        for day in self.forecasts:
            date = day.get('date')
            hours = day.get('hours')
            for hour_dict in hours:
                hour = hour_dict.get('hour')
                temperature = hour_dict.get('temp')
                condition = hour_dict.get('condition')

                if int(hour) in TIME_LIST:
                    self.temperatures[date] = temperature

                if condition in GOOD_WEATHER:
                    self.no_precipitation_time += 1

    def get_calculation_data(self):
        self._calculation()
        avg_tem = self._get_avg_temp()
        return CalculationData(
            city=self.city,
            days=self.temperatures,
            avg_tem=avg_tem,
            no_precipitation_time=self.no_precipitation_time,
        )

    def _get_avg_temp(self):
        return round(statistics.fmean(self.temperatures.values()))


@dataclass
class DataAnalyzingTask:
    all_cites_data: list

    def add_rating(self):
        # рейтинг будем считать по максимальной сумме средней температуры и числа солнечных дней
        sorted_data = sorted(self.all_cites_data, key=lambda x: x.avg_tem + x.no_precipitation_time)
        [setattr(data, 'rating', num + 1) for num, data in enumerate(sorted_data)]
        return sorted_data


class DataAggregationTask:

    def __init__(self, data, lock):
        self.lock = lock
        self.data = self._set_dates(data)

    @staticmethod
    def _set_dates(data):
        dates = data.days
        dict_data = asdict(data)

        for date, tem in dates.items():
            dict_data[date] = tem

        dict_data.pop('days', None)
        return dict_data

    def write_to_csv(self):
        with open(CSV_FILENAME, 'a') as csvfile:
            with self.lock:
                fieldnames = self.data.keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if os.stat(CSV_FILENAME).st_size == 0:
                    writer.writeheader()

                writer.writerow(self.data)
