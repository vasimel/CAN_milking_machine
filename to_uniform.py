import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import sys



def convert_to_timedelta(time_str):
    minutes, seconds = map(int, time_str.split(':'))
    return timedelta(minutes=minutes, seconds=seconds)



def to_uniform(file_path):

    filename = os.path.basename(file_path)
    dirname = os.path.dirname(file_path)

    now = datetime.now()
    current_date = now.strftime('%d.%m.%Y')

    df = pd.read_csv(file_path, sep='\t')
    df['Надой'] = df['Надой'].str.replace(',', '.').apply(pd.to_numeric, errors='coerce')
    df['Температура'] = df['Температура'].str.replace(',', '.').apply(pd.to_numeric, errors='coerce')
    df['Время дойки'] = df['Время дойки'].apply(convert_to_timedelta)
    df['Аппарат'] = df['Аппарат'].astype(str)

    df = df.groupby('Номер коровы').agg({
    'Аппарат': lambda x: ' '.join(x),  # Объединяем значения в строку
    'Надой': 'sum',  # Суммируем надой
    'Время дойки': 'sum',  # Суммируем время дойки
    'Температура': 'max',  # Выбираем максимальную температуру
    'Тревоги': lambda x: ', '.join(x)  # Суммируем количество тревог
    }).reset_index()

    df['Надой'] = df['Надой'].astype(str).str.replace('.', ',')
    df['Температура'] = df['Температура'].astype(str).str.replace('.', ',')
    df['Время дойки'] = df['Время дойки'].apply(lambda x: f"{x.seconds // 60:02d}:{x.seconds % 60:02d}")


    df['Жир'] = np.nan
    df['Белок'] = np.nan
    df['Лактоза'] = np.nan
    df['Дата'] = current_date
    new_order = ['Номер коровы', 'Жир', 'Белок', 'Надой', 'Лактоза', 'Дата', 'Время дойки', 'Температура', 'Тревоги', 'Аппарат']
    df = df[new_order]
    
    new_file_name = f"uniform_{filename}."

    df.to_csv(os.path.join(dirname, new_file_name), sep=';', index=False)


to_uniform(sys.argv[1])
