#! /usr/bin/python3

import smbus
import sys
import os
import can
import time
import signal

from logger import logger as logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Process, current_process, Queue, Manager, shared_memory
import threading
import canfd
import queue
from concurrent.futures import wait
import numpy as np
# from konfig import Config
import configparser
import unitboard
from unitboard import UnitBoard as unit_board
from client import TcpClientThread as tcp_client

shared_object = Manager().list()            # 프로세스간 공유 소켓 정보
shared_object.insert(0, None)

GPIOADDR = 0x20
MAXUNITBOARD = 1
   
class CosmoMain(threading.Thread):
    def __init__(self, tcp_queue, config_file) -> None:
        global GPIOADDR, MAXUNITBOARD
        
        threading.Thread.__init__(self) 
        os.system("sudo ifconfig can0 down")
        time.sleep(0.1)
        # os.system("sudo ip link set can0 up type can bitrate 1000000 dbitrate 8000000 restart-ms 1000 berr-reporting on fd on sample-point .8 dsample-point .8")
        os.system("sudo ip link set can0 up type can bitrate 1000000 dbitrate 4000000 restart-ms 1000 berr-reporting on fd on")
        os.system("sudo ifconfig can0 txqueuelen 65536")
        
        self.client = None
        self.tcp_queue = tcp_queue
        self.common_config = config_file
        common_config = self.common_config['common']
        MAXUNITBOARD = int(common_config['MAXUNITBOARD'])
        GPIOADDR1 = int(common_config['GPIOADDR1'], 16)           #16 진수
        GPIOADDR2 = int(common_config['GPIOADDR2'], 16)           #16 진수
        
        filters = [
            {"can_id": 0x100, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x101, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x102, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x103, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x104, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x105, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x106, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x107, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x108, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x109, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10A, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10B, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10C, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10D, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10E, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x10F, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x110, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x111, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x112, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x113, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x114, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x115, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x116, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x117, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x118, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x119, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11A, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11B, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11C, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11D, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11E, "can_mask": 0x7FF, "extended": False},
            {"can_id": 0x11F, "can_mask": 0x7FF, "extended": False},
        ]
          
        self.can0 = can.interface.Bus(rx_fifo_size = 8192, channel = 'can0', interface = 'socketcan', bitrate_switch = True, bitrate = 1000000, data_bitrate = 4000000, fd = True, can_filters=filters)  # socketcan_native
        
        self.i2cbus = smbus.SMBus(1) 
        self.i2cbus.write_byte_data(GPIOADDR1, 0x00, 0x00)        # OUTPUT
        self.i2cbus.write_byte_data(GPIOADDR1, 0x01, 0x00)        # OUTPUT
        self.i2cbus.write_byte_data(GPIOADDR1, 0x12, 0xFF)
        self.i2cbus.write_byte_data(GPIOADDR1, 0x13, 0xFF)
        
        self.i2cbus.write_byte_data(GPIOADDR2, 0x00, 0x00)        # OUTPUT
        self.i2cbus.write_byte_data(GPIOADDR2, 0x01, 0x00)        # OUTPUT
        self.i2cbus.write_byte_data(GPIOADDR2, 0x12, 0xFF)
        self.i2cbus.write_byte_data(GPIOADDR2, 0x13, 0xFF)
        self.command_queue = []
        self.i2cbus.close()
        
        self.unit_np_shm = None
        self.unit_semaphor = None
        
    def run(self):
        while True:   
            message = self.tcp_queue.get()  
            # if  message['CMD'] != 'GET_STATUS' and message['CMD'] != 'PING':         # GET_STATUS는 계속 호출 되므로 log에 출력 하지 않음.
            #     if message['CMD'] == 'REF':
            #         logging.info(f"{message['CMD']} command is inserted to {message['TANK_ID']} Unit Board")
            #     elif message['CMD'] == 'STATE':
            #         logging.info(f"{message['CMD']} command is inserted Unit Board")
            #     else:
            #         logging.info(f"{message['CMD']} command is inserted Unit Board")
            # UNIT_ID는 관리 프로그램과 협의해서 변경해야 함.
            if message['CMD'] == 'STATE':
                for x in range(MAXUNITBOARD):
                    message['UNIT_ID'] = str(int(message['DATA'][x]['TANK_ID']) - 100)
                    self.command_queue[x].put(message, block=False)
            elif message['CMD'] == 'REF':
                matching = False
                for x in range(MAXUNITBOARD):
                    config = self.common_config[f'unit_board{x}']
                    if message['TANK_ID'] == str(int(config['ADDRESS']) + 100):
                        message['UNIT_ID'] = str(int(message['TANK_ID']) - 100)
                        self.command_queue[x].put(message, block=False)
                        matching = True
                if not matching:
                    logging.info(f"Wrong Unit board id{message['TANK_ID']}")
            elif message['CMD'] == 'SET_MOTOR':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'SET_GPIO':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'GET_ADC':  
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False )
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'GET_STATUS':  
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'START_TEMP':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'STOP_TEMP':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'TEMP_RPM':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'CTRL':
                matching = False
                for x in range(MAXUNITBOARD):
                    config = self.common_config[f'unit_board{x}']
                    if message['TANK_ID'] == str(int(config['ADDRESS']) + 100):
                        message['UNIT_ID'] = str(int(message['TANK_ID']) - 100)
                        self.command_queue[x].put(message, block=False)
                        matching = True
                time.sleep(0.05)
                
                if not matching:
                    logging.info(f"Wrong Unit board id{message['TANK_ID']}")
            elif message['CMD'] == 'FIRMWARE_UPDATE':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:       # 0 ~ MAXUNITBOARD-1
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")
            elif message['CMD'] == 'GET_VERSION':
                if message['UNIT_ID'] >= 0 and message['UNIT_ID'] < MAXUNITBOARD:       # 0 ~ MAXUNITBOARD-1
                    self.command_queue[message['UNIT_ID']].put(message, block=False)
                else:
                    logging.info(f"Wrong Unit board id{message['UNIT_ID']}")        
def main():
    config_file = configparser.ConfigParser()  ## 클래스 객체 생성
    config_file.read('/home/pi/Projects/cosmo-m/config/config.ini')  ## 파일 읽기
    common_config = config_file['common']
    
    tcp_queue = queue.Queue(maxsize=8192)
    main_func = CosmoMain(tcp_queue, config_file)

    global MAXUNITBOARD, ADDRESS
    MAXUNITBOARD = int(common_config['MAXUNITBOARD'])
    GPIOADDR1 = int(common_config['GPIOADDR1'], base=16)
    GPIOADDR2 = int(common_config['GPIOADDR2'], base=16)
    
    # ip = common_config['HOST']
    # port = int(common_config['PORT2'])   
    manager = Manager()
    
    can_fd_receive = canfd.CanFDReceive(logging, main_func)
    can_fd_receive.start()
    i2c_semaphor = manager.Semaphore(1) 
    
    can_fd_transmitte = canfd.CanFDTransmitte(logging, main_func)
    can_fd_transmitte.queue = manager.Queue(1024)
    can_fd_transmitte.start()
    socket_send_queue = manager.Queue(524288)
    status_control_queue = manager.Queue(1024)
    
    for i in range(MAXUNITBOARD):
        can_fd_receive.receive_queue.append(manager.Queue(1024))
        main_func.command_queue.append(manager.Queue(1024))
    
    # 숫자를 저장할 numpy 배열(1차원) 생성
    shared_mem_size = int(common_config['SHARED_MEMORY_SIZE'])
    arr = np.array([i for i in range(MAXUNITBOARD * shared_mem_size)], dtype=np.int32) 
    # 공유 메모리 생성
    shm = shared_memory.SharedMemory(create=True, size=arr.nbytes)
    # 공유 메모리의 버퍼를 numpy 배열로 변환
    main_func.unit_np_shm = np.ndarray(arr.shape, dtype=arr.dtype, buffer=shm.buf)
    main_func.unit_semaphor = manager.Semaphore(1) 
    main_func.start()
    
    socket_event = threading.Event()
    main_func.client = tcp_client(tcp_queue, logging, GPIOADDR1, GPIOADDR2, socket_event, i2c_semaphor, MAXUNITBOARD, 
                                  shm.name, main_func.unit_np_shm, socket_send_queue, status_control_queue)
    main_func.client.start()                            #tcp client 시작
    unitboard.g_file_path = common_config['JSON_FILE']
    # print("Server is not Connected")
    try:
        while True:
            # if shared_object[0] and not shared_object[0]._closed:           # 처음 서버에 연결 될 때까지 무한루프 실행
            print("Server is not Connected")
            socket_event.wait()
            socket_event.clear()
            print("Server is Connected")
            with ProcessPoolExecutor(max_workers=32) as executor:
                unit_func = unit_board(can_fd_transmitte.queue, socket_send_queue, GPIOADDR1, GPIOADDR2, i2c_semaphor, status_control_queue)
                
                furtures = {executor.submit(unit_func.unit_process, i, shm.name, main_func.unit_np_shm, 
                                            main_func.unit_semaphor, can_fd_receive.receive_queue[i],
                                            main_func.command_queue[i], logging) : i for i in range(MAXUNITBOARD)}
                for furture in as_completed(furtures):  
                    print("All Process is done")
                sys.exit(1)
            # else:
            #     time.sleep(1)
            #     print("Server is not Connected")
    except Exception as e:
        main_func.can0.shutdown()
        shm.close()
        shm.unlink()
        for i in range(MAXUNITBOARD):
            os.kill(main_func.unit_np_shm[i*shared_mem_size + 30], signal.SIGKILL)
        print(e)
if __name__ == "__main__":
    main()
    