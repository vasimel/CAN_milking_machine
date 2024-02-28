import csv

def write_to_csv(data, filename):
    # Открытие файла для записи
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=";")

        # Запись заголовков столбцов
        writer.writerow(['Аппарат', 'Надой (л)', 'Время (мин)', 'Температура', 'Номер коровы', 'Тревоги'])

        # Проход по каждому элементу словаря
        for machine_id, records in data.items():
            # Запись каждой записи для данного machine_id
            for record in records:
                writer.writerow([machine_id] + record)
