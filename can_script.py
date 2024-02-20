import can
import threading
import queue

nodes_to_query = queue.Queue()

class newNode_Listener(can.Listener):
    def on_message_received(self, msg):
        arbit_id = msg.arbitration_id
        if arbit_id >= 0x700 and arbit_id <= 0x718:
            node_id = arbit_id - 0x700
            nodes_to_query.put(node_id)

class Answer_Listener(can.Listener):
    def __init__(self, response_event):
        self.response_event = response_event
        self.expected_arbitration_id = None
        self.received_message = None

    def set_expected_arbitration_id(self, arbitration_id):
        self.expected_arbitration_id = arbitration_id
        self.received_message = None

    def on_message_received(self, msg):
        if msg.arbitration_id == self.expected_arbitration_id:
            self.received_message = msg
            self.response_event.set()



milking_query_data = [[0x40, 0x00, 0x21, 0x02], [0x2F, 0x00, 0x21, 0x02], 
                      [0x40, 0x00, 0x21, 0x03], [0x40, 0x00, 0x21, 0x04],
                      [0x40, 0x00, 0x21, 0x05], [0x40, 0x00, 0x21, 0x06],
                      [0x40, 0x00, 0x21, 0x07]]
milking_answer_data = [[0x4F, 0x00, 0x21, 0x02], [0x60, 0x00, 0x21, 0x02], 
                       [0x4B, 0x00, 0x21, 0x03], [0x4B, 0x00, 0x21, 0x04],
                       [0x4B, 0x00, 0x21, 0x05], [0x4B, 0x00, 0x21, 0x06],
                       [0x4B, 0x00, 0x21, 0x07]]

def main():
    bus = can.interface.Bus(bustype='ixxat', channel='0', bitrate=125000)
    logger = can.writer.CSVWriter('can_log.csv')
    newnode_listener = newNode_Listener()
    response_event = threading.Event()
    answer_listener = Answer_Listener(response_event)
    notifier = can.Notifier(bus, [newnode_listener, answer_listener, logger])

    message = can.Message(arbitration_id=0x80, data=[], is_extended_id=False)
    while True:
        try:
            bus.send(message)
            break
        except can.CanError as e:
            pass


    task = can.broadcastmanager.CyclicSendTask(bus, message, 0.1)
    task.start()

    if not nodes_to_query.empty():
         while not nodes_to_query.empty():
            node = nodes_to_query.get()
            arbit_id = 0x600 + node
            answer_listener.set_expected_arbitration_id(0x580 + node)
            query_message = can.Message(arbitration_id=arbit_id, data=[0x40, 0x01, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00], is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()
            query_message = can.Message(arbitration_id=arbit_id, data=[0x40, 0x00, 0x21, 0x01, 0x00, 0x00, 0x00, 0x00], is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()
            number_of_milkings = int(answer_listener.received_message.data[4], 10)
            query_message = can.Message(arbitration_id=arbit_id, data=[0x40, 0x18, 0x10, 0x02, 0x00, 0x00, 0x00, 0x00], is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            response_event.clear()
            for i in range(number_of_milkings):
                for m in milking_query_data:
                    query_message = can.Message(arbitration_id=arbit_id, data=m + [0x00, 0x00, 0x00, 0x00], is_extended_id=False)
                    if m[0] == 0x2F:
                        query_message.data[4] = i #непонятно как тут быть
                    bus.send(query_message)
                






    notifier.stop()

    task.stop()

    logger.stop()

    
if __name__ == "__main__":
    main()


