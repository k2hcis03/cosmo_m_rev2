#! /usr/bin/python3
import os
import can
import time

from concurrent.futures import ThreadPoolExecutor
import threading

class CanFDReceive(threading.Thread):
    def __init__(self, logging, main_func) -> None:
        threading.Thread.__init__(self) 
        self.can0 = main_func.can0
        self.logging = logging
        self.daemon = True
        self.receive_queue = []
        self.logging.info('CanFDReceive initialized')
        
    def run(self): 
        while True:
            message = self.can0.recv()
            # self.can0.shutdown()
            if message.arbitration_id >= 0x300 and message.arbitration_id <= 0x31F:
                self.receive_queue[message.arbitration_id - 0x300].put(message)
            else:
                self.logging.warning(f'Wrong arbitration_id {message.arbitration_id}')

class CanFDTransmitte(threading.Thread):
    def __init__(self, logging, main_func) -> None:
        threading.Thread.__init__(self) 
        self.can0 = main_func.can0
        self.logging = logging
        self.logging.info('CanFDTransmitte initialized')
        self.daemon = True
        self.queue = None
        
    def run(self): 
        while True:
            message = self.queue.get()
            self.can0.send(message)
