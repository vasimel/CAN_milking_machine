import can
import threading
import queue
from csv_logger import write_to_csv, to_uniform
from time import sleep
from datetime import datetime
import locale 
import os

# Получение текущей даты и времени
now = datetime.now()

# Преобразование в строку в формате 'гггг-мм-дд чч:мм:сс'
date_time_str = now.strftime("%d-%m-%Y_%H-%M-%S")
date_str = now.strftime('%d.%m.%Y')
nodes_to_query = queue.Queue()



class newNode_Listener(can.Listener):
    def __init__(self, newnode_event = None, output_func = None):
        self.newnode_event = newnode_event
        self.output_func = output_func
    def on_message_received(self, msg):
        arbit_id = msg.arbitration_id
        if arbit_id >= 0x700 and arbit_id <= 0x718:
            node_id = arbit_id - 0x700
            nodes_to_query.put(node_id)
            self.output_func(f"Добавлен аппарат {node_id} ({hex(node_id)})")
            
            

class Answer_Listener(can.Listener):
    def __init__(self, response_event = None):
        self.response_event = response_event
        self.expected_arbitration_id = None
        self.received_message = None

    def set_expected_arbitration_id(self, arbitration_id):
        self.expected_arbitration_id = arbitration_id
        self.received_message = None

    def on_message_received(self, msg):
        if msg.arbitration_id == self.expected_arbitration_id:
            self.received_message = msg
            if self.response_event is not None:  # Проверка, что response_event не None перед вызовом set()
                self.response_event.set()


milking_query_data = [[0x50, 0x00, 0x22, 0x02], [0x2A, 0x00, 0x20, 0x02], 
                      [0x50, 0x00, 0x22, 0x03], [0x40, 0x00, 0x22, 0x04],
                      [0x50, 0x00, 0x22, 0x05], [0x40, 0x00, 0x22, 0x06],
                      [0x50, 0x00, 0x21, 0x08]]
milking_answer_data = [[0x4F, 0x00, 0x21, 0x02], [0x60, 0x00, 0x21, 0x02], 
                       [0x4C, 0x00, 0x21, 0x03], [0x4C, 0x00, 0x21, 0x04],
                       [0x4C, 0x00, 0x21, 0x05], [0x4C, 0x00, 0x21, 0x06],
                       [0x4C, 0x00, 0x21, 0x07]]

machines = dict()
#machines = {1:[1, 2, 3, 4][4,5,6,7]...}
def main(output_func):

    bus = can.interface.Bus(bustype='ixxat', channel='0', bitrate=125000,
                             receive_own_messages = True)
    logger = can.CSVWriter('can_log.csv')

    newnode_event = threading.Event()
    newnode_listener = newNode_Listener(newnode_event, output_func)

    response_event = threading.Event()
    answer_listener = Answer_Listener(response_event)

    notifier = can.Notifier(bus, [newnode_listener, answer_listener, logger])

    wakeup_message = can.Message(arbitration_id=0x80, data=[], is_extended_id=False)
    

    if nodes_to_query.empty():
        output_func(f"Ожидание подключения аппаратов...")
        newnode_event.wait(timeout=30)
        sleep(3)

    if not nodes_to_query.empty():
        output_func(f"Начинаю опрос...")
        while not nodes_to_query.empty():
            node = nodes_to_query.get()
            arbit_id = 0x600 + node
            answer_listener.set_expected_arbitration_id(0x580 + node)
            query_message = can.Message(arbitration_id=arbit_id, 
                                        data=[0x50, 0x01, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00], #неизвестный запрос
                                        is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()

            query_message = can.Message(arbitration_id=arbit_id, 
                                        data=[0x50, 0x00, 0x21, 0x01, 0x00, 0x00, 0x00, 0x00], #количество доек, номер аппарата
                                        is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()
            number_of_milkings = answer_listener.received_message.data[4]
            machine_id = answer_listener.received_message.data[5]
            machines[machine_id] = []

            query_message = can.Message(arbitration_id=arbit_id, 
                                        data=[0x50, 0x10, 0x10, 0x02, 0x00, 0x00, 0x00, 0x00], #неизвестный запрос
                                        is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()
            #print(number_of_milkings)
            for i in range(number_of_milkings):
                milking_data = []
                for m in range(len(milking_query_data)):
                    query_message = can.Message(arbitration_id=arbit_id, data=milking_query_data[m] + [0x00, 0x00, 0x00, 0x00], is_extended_id=False)
                    if m == 1:
                        query_message.data[4] = int(hex(i), 16)
                    bus.send(query_message)
                    response_event.wait()
                    response_event.clear()
                    #assert(answer_listener.received_message.data[:4] == milking_answer_data[m])
                    data = answer_listener.received_message.data[4:6]
                    if m == 2: #молоко
                        milk = float((data[1] << 8) | data[0]) / 100
                        milking_data.append(locale.str(milk))
                    elif m == 3: #время
                        time = f"{data[1]:02d}:{data[0]:02d}"
                        milking_data.append(time)
                    elif m == 4: #темпа
                        temp = float((data[1] << 8) | data[0]) / 10
                        milking_data.append(locale.str(temp))
                    elif m == 5: #номер коровы
                        cow_id = (data[1] << 8) | data[0]
                        milking_data.append(cow_id)
                    elif m == 6: #тревоги 
                        alarm = data[0]
                        if alarm == 0x01 or alarm == 0x03:
                            alarm_msg = f"HMC-{alarm}"
                        elif alarm == 0x80:
                            alarm_msg = "Temperature"
                        else:
                            alarm_msg = 0
                        milking_data.append(alarm_msg)
                    else:
                        pass

                machines[machine_id].append(milking_data)


            output_func(f"Аппарат {machine_id} ({hex(machine_id)}): выгружено {len(machines[machine_id])} записей")
        #----тут очистка----#

#            if number_of_milkings:
 #               query_message = can.Message(arbitration_id=arbit_id, 
  #                                      data = [0x40, 0x00, 0x21, 0x02, 0x00, 0x00, 0x00, 0x00], is_extended_id=False) #запрос номера
   #             bus.send(query_message)
    #            response_event.wait()
     #           response_event.clear()
      #          query_message = can.Message(arbitration_id=arbit_id, 
       #                                 data = [0x2F, 0x00, 0x21, 0x02, 0xFF, 0x00, 0x00, 0x00], is_extended_id=False) #erase
        #        bus.send(query_message)
         #       response_event.wait()
          #      response_event.clear()
           #     output_func(f"Данные аппарата {machine_id} очищены")


    logfile_name = f"{date_time_str}.csv"
    # Путь к папке
    folder_path = r'D:\download_reports\reports'

    # Создаем папку, если она не существует
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Путь к файлу внутри папки
    file_path = os.path.join(folder_path, logfile_name)

    write_to_csv(machines, file_path)

    to_uniform(file_path, date_str)

    output_func(f"Опрос завершен. Данные загружены в файл {file_path}.")

    notifier.stop()

    #task.stop()

    logger.stop()

    bus.shutdown()


if __name__ == "__main__":
    main()


