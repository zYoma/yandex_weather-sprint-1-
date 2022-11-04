import multiprocessing
import os
import threading
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)

from tasks import (CSV_FILENAME, DataAggregationTask, DataAnalyzingTask,
                   DataCalculationTask, DataFetchingTask)
from utils import CITIES

if __name__ == '__main__':
    pool_size = multiprocessing.cpu_count()
    # получаем данные по каждому городу в отдельном потоке
    data_fetching = DataFetchingTask()
    with ThreadPoolExecutor(max_workers=pool_size) as pool:
        cities_data = pool.map(data_fetching.get_weather, CITIES)

    # делаем вычисления по каждому городу в отдельном процессе
    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        calculation_data = {
            executor.submit(DataCalculationTask(city, data).get_calculation_data): data
            for city, data in cities_data
        }
    calculation_data_result = [future.result() for future in as_completed(calculation_data)]
    # проставляем рейтинги городам
    sorted_by_rating = DataAnalyzingTask(calculation_data_result).add_rating()
    # Очищаем файл перед наполнением новыми данными
    if os.path.exists(CSV_FILENAME):
        os.remove(CSV_FILENAME)
    # Пишем в файл с нескольких потоков, используем эксклюзивный доступ через лок
    thread_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        future_to_url = {
            executor.submit(DataAggregationTask(data, thread_lock).write_to_csv): data
            for data in sorted_by_rating
        }

    print(f"Самый теплый город: {sorted_by_rating[-1].city}")
