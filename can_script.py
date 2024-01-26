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





def main():
    bus = can.interface.Bus(bustype='ixxat', channel='0', bitrate=125000)
    newnode_listener = newNode_Listener()
    response_event = threading.Event()
    answer_listener = Answer_Listener(response_event)
    notifier = can.Notifier(bus, [newnode_listener, answer_listener])

    message = can.Message(arbitration_id=0x80, data=[], is_extended_id=False)
    while True:
        try:
            bus.send(message)
            break
        except can.CanError as e:
            pass


    task = can.broadcastmanager.CyclicSendTask(bus, message, 0.1)
    task.start()
    response_event = threading.Event()

    if not nodes_to_query.empty():
         while not nodes_to_query.empty():
            node = nodes_to_query.get()
            arbit_id = 0x600 + node
            answer_listener.set_expected_arbitration_id(580 + node)
            query_message = can.Message(arbitration_id=arbit_id, data=[0x40, 0x01, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00], is_extended_id=False)
            bus.send(query_message)
            response_event.wait()
            number_of_milkings = int(answer_listener.received_message.data[5], 10)
            for i in range(number_of_milkings):
                pass #опрос по характеристикам






    notifier.stop()

    task.stop()

if __name__ == "__main__":
    main()


