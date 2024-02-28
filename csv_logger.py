import csv
import pandas as pd
import numpy as np
import os


def write_to_csv(data, filename):
    # Открытие файла для записи
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=";")

        # Запись заголовков столбцов
        writer.writerow(['Аппарат', 'Надой (л)', 'Время дойки', 'Температура', 'Номер коровы', 'Тревоги'])

        # Проход по каждому элементу словаря
        for machine_id, records in data.items():
            # Запись каждой записи для данного machine_id
            for record in records:
                writer.writerow([machine_id] + record)


# Шаг 1: Считываем файл CSV в DataFrame
def to_uniform(file_path, current_date):

    filename = os.path.basename(file_path)
    dirname = os.path.dirname(file_path)

    df = pd.read_csv(file_path, sep=';')
    df['Жир'] = np.nan
    df['Белок'] = np.nan
    df['Лактоза'] = np.nan
    df['Дата'] = current_date
    new_order = ['Номер коровы', 'Жир', 'Белок', 'Надой (л)', 'Лактоза', 'Дата', 'Время дойки', 'Температура', 'Тревоги', 'Аппарат']
    df = df[new_order]
    new_file_name = f"uniform_{filename}"

    df.to_csv(os.path.join(dirname, new_file_name), sep=';', index=False)
