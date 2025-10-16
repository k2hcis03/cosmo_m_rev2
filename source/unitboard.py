#! /usr/bin/python3

import smbus
import os
import can
import time
import numpy as np
import json
from multiprocessing import Process, Queue, Manager, shared_memory
import smbus 
import threading
from multiprocessing import Queue
from datetime import datetime
from threading import Timer
from pid_controller import PID_COSMO_M
import csv
import configparser
import traceback
import datetime

setpoint = 15.0  # Target temperature
Kp = 1.0  # Proportional gain
Ki = 0.5  # Integral gain
Kd = 0.1  # Derivative gain

ON = 1
OFF = 0
# g_file_path = "./data/JSON_Ref_Stage101.txt"
                   
class UnitBoardTempControl(threading.Thread):
    def __init__(self, id, event, logging, can_fd_transmitte_queue, 
                 command_queue, shared_memory, unit_semaphor, config, shared_memory_size):
        threading.Thread.__init__(self)
        self.daemon = True
        self.id = id                            # id는 0부터 시작
        self.logging = logging
        self.can_fd_transmitte_queue = can_fd_transmitte_queue
        self.event = event
        self.pid_timer_event = threading.Event()
        self.command_queue = command_queue
        self.pid = PID_COSMO_M(Kp, Ki, Kd, setpoint=setpoint)
        # Set output limits (heating/cooling power)
        self.pid.output_limits = (0, 50)
        self.pid.sample_time = 5  # Update every 0.01 seconds

        self.shared_memory_u = shared_memory
        self.shared_memory_size = shared_memory_size
        self.time_to_on = 0                 # Valve 릴레이가 ON되는 시간 변수
        self.pid_timer_call_time = 0        # pid or timer 계산 시간 5초. 0.1초 단위이므로 50
        self.unit_semaphor = unit_semaphor
        
        self.config = config
        
        self.check_time = 0
        self.cold_valve_status = 0
        self.temp_control_start = False

        self.ref_datas = []                 # 서버에서 전송되는 REF 데이터 모든 것을 저장하는 리스트
        self.ref_stage = 0
        self.ref_step = 0
        self.ref_data = []                  # REF 데이터 중에서 data항목 저장
        self.ref_total = 0
    
        self.file_write = False             # CSV 파일을 새로 만들지 결정
        self.file_write_state = None        # CSV 파일 저장 조건 상태
        self.file_index = 0
        self.timer_control_valve = False    # 타이머로 제어 할 때, 시간 마다 한 번만 제어 하기위한 변수
        self.motor_rpm = 0                  # 모터 현재 속도
        self.dir_name = None                # data 기록 디렉토리 생성 initial 상태에서 업데이트 됨
    
    def set_cold_valve(self, value):
        self.cold_valve_status = value
        x = self.config["SOLVALVE2"]        #냉각수 밸브 I/O 번호
        message = {"UNIT_ID" : self.id,                  
                    "CMD":"TEMP_VALVE",
                    "CHANNEL": x,
                    "VALUE" : value}
        self.command_queue.put(message, block=False) 
    
    def pid_task(self):    
        if self.pid_timer_call_time > 0 and self.temp_control_start:            
            if self.time_to_on:
                self.time_to_on -= 1
                if self.cold_valve_status == 0:
                    self.set_cold_valve(ON)
            else:
                if self.cold_valve_status == 1:
                    self.set_cold_valve(OFF) 
            self.pid_timer_call_time -= 1
            Timer(0.1, self.pid_task).start()
        else:
            Timer(0.1, self.pid_task).cancel()
            self.pid_timer_event.set()
            
    def timer_task(self):    
        if self.pid_timer_call_time > 0 and self.temp_control_start:            
            if self.time_to_on > 0 and self.timer_control_valve:
                self.time_to_on -= 1
                if self.cold_valve_status == 0:
                    self.set_cold_valve(ON)
                    # 온도 제어할 때, 모터가 100rpm보다 느린상태라면 구동 시킴
                    ref_rpm = int(self.config["TEMP_CONTROL_MOTOR_RPM"])
                    ref_motor_time = int(self.config["TEMP_CONTROL_MOTOR_TIME"])
                    if self.motor_rpm < 100:                      
                        message = {"UNIT_ID" : self.id,                  
                                        "CMD":"TEMP_RPM",
                                        "SPEED" : ref_rpm, 
                                        "DIR"   : 'FW',            #FW = forward, RV = reverse
                                        "ONOFF" : 'ON', 
                                        "TIME" : ref_motor_time,
                                        "SEND" : False}    
                        self.command_queue.put(message, block=False) 
                        # self.logging.info(f"{message['CMD']} command is inserted Unit Board")
            else:
                if self.cold_valve_status == 1:
                    self.set_cold_valve(OFF) 
                    self.timer_control_valve = False
            self.pid_timer_call_time -= 1
            Timer(1, self.timer_task).start()
        else:
            Timer(1, self.timer_task).cancel()
            
            if self.cold_valve_status == 1:
                self.set_cold_valve(OFF) 
            # 한시간 마다 온도를 측정해서 냉각수를 구동시키기 때문에 위 if 문이 참이 아니면 다음 한시간을 기다리기 위해 아래
            # self.timer_control_valve = False를 수행함
            self.timer_control_valve = False
            self.pid_timer_event.set()
              
    def run(self):
            self.logging.info(f'id : {self.id} UnitBoard Temp Control Thread Run')
            while True:
                try:
                    self.event.wait()
                    self.event.clear()
                    # self.pid.reset() 테스트 필요
                    ##############################################################################################################
                    ## 온도 관련 동작
                    ## 20230609
                    ## @K2H
                    ## 온도 테스트를 위한 데이저 저장. 
                    if self.file_write:
                        try:
                            if not os.path.isdir(self.dir_name):
                                os.mkdir(self.dir_name)
                            self.writer_csv = open(f'{self.dir_name}/pid_process{self.id}_{self.file_index}.csv', 'w', encoding='utf-8', newline='')
                            self.writer = csv.writer(self.writer_csv, delimiter=',')
                            self.writer.writerow(['time'] + ['ref.temp'] + ['real temp'] + ['valve on time']  + ['ext1 temp'] + ['ext1 humi'] + 
                                                 ['ext2 temp'] + ['ext2 humi'] + ['relay1_water'] + ['relay2_cold'] + ['relay3_res1'] + ['relay4_res2'] + ['relay4_res3'] + 
                                                 ['rpm'] + ['analog1_up'] + ['analog3_res1'] + ['analog4_res2'] + ['analog5_res3'] +['analog6_res4'])   
                            #real temp == analog2, 
                            self.file_write = False
                        except Exception as e:
                            print(e)
                    ##############################################################################################################
                    for x in range(self.ref_total):
                        if self.temp_control_start:
                            time_start = time.time()
                            ref_temp = float(self.ref_data[x]["TEMP"])
                            ref_rpm = int(self.ref_data[x]["M_RPM"])
                            ref_motor_time = int(self.ref_data[x]["M_TIME"])
                            ref_temp_error = float(self.ref_data[x]["TEMP_MGN"])
                            self.pid.modify_setpoint(ref_temp)
                            
                            # 2023-08-11 테스트용으로 모터 RPM을 1000으로 수정
                            # ref_rpm = 1000
                            #################################################
                            # 레퍼런스 데이터에서 모터를 구동시키면 아래 동작함.
                            message = {"UNIT_ID" : self.id,                  
                                        "CMD":"TEMP_RPM",
                                        "SPEED" : ref_rpm, 
                                        "DIR"   : 'FW',            #FW = forward, RV = reverse
                                        "ONOFF" : 'ON', 
                                        "TIME" : ref_motor_time,
                                        "SEND" : False}    
                            self.command_queue.put(message, block=False) 
                            # self.logging.info(f"{message['CMD']} command is inserted Unit Board")
                            # self.logging.info(f'id : {self.id} UnitBoard Temp Control Thread {x} Step Start at {time_start} Time')
                            self.timer_control_valve = True     # ref_step마다 한번씩 ON 해준다.
                            
                            while (time_start + self.ref_step) > time.time():
                                if self.temp_control_start:
                                    # client.py에서 data = {"unit_id" : x , "cmd":"GET_STATUS", "send" : False, "raw" : False}
                                    # 로 데이터를 보내므로 온도 값이 계산되어 저장됨 따라서 *0.01을 하면 온도 값으로 사용
                                    current_temp2 = self.shared_memory_u[0x11 + self.id*self.shared_memory_size] * 0.01 #온도 센서 2
                                    current_ext_temp1 = self.shared_memory_u[0x0C + self.id*self.shared_memory_size] * 0.1 #Ext Temp1
                                    current_ext_humi1 = self.shared_memory_u[0x0D + self.id*self.shared_memory_size] * 0.1 #Ext Humi1
                                    current_ext_temp2 = self.shared_memory_u[0x0E + self.id*self.shared_memory_size] * 0.1 #Ext Temp2
                                    current_ext_humi2 = self.shared_memory_u[0x0F + self.id*self.shared_memory_size] * 0.1 #Ext Humi2
                                    self.motor_rpm = self.shared_memory_u[0x0A + self.id*self.shared_memory_size]          #RPM
                                    
                                    
                                    analog1 = self.shared_memory_u[0x10 + self.id*self.shared_memory_size] * 0.01 #온도 센서 1
                                    analog3 = self.shared_memory_u[0x12 + self.id*self.shared_memory_size] * 0.01 #온도 센서 3
                                    analog4 = self.shared_memory_u[0x13 + self.id*self.shared_memory_size] * 0.01 #온도 센서 4
                                    analog5 = self.shared_memory_u[0x14 + self.id*self.shared_memory_size] * 0.01 #온도 센서 5
                                    analog6 = self.shared_memory_u[0x15 + self.id*self.shared_memory_size] * 0.01 #온도 센서 6
                                    
                                    gpio1 = (self.shared_memory_u[0x06 + self.id*self.shared_memory_size]) & 0xFF
                                    gpio2 = (self.shared_memory_u[0x06 + self.id*self.shared_memory_size] >> 8) & 0xFF
                                    gpio3 = (self.shared_memory_u[0x06 + self.id*self.shared_memory_size] >> 16) & 0xFF
                                    gpio4 = (self.shared_memory_u[0x06 + self.id*self.shared_memory_size] >> 24) & 0xFF
                                    gpio5 = (self.shared_memory_u[0x07 + self.id*self.shared_memory_size]) & 0xFF
                                    
                                    if self.file_write_state:                               #STATE가 Run이면 True Pause면 False
                                        self.writer.writerow([time.time(), ref_temp, current_temp2, self.time_to_on, current_ext_temp1, current_ext_humi1, 
                                                              current_ext_temp2, current_ext_humi2, gpio1, gpio2, gpio3, gpio4, gpio5, self.motor_rpm, 
                                                              analog1, analog3, analog4, analog5, analog6])
                                        print(f'id: {self.id} period: {time.time()} time to on: {self.time_to_on} C.T: {current_temp2:0.2F} and F.T: {ref_temp}')
                                        
                                        # print(f'id: {self.id} analog1: {analog1} analog3: {analog3} analog4: {analog4} analog5: {analog5} and analog6: {analog6}')
                                        # print(f'id: {self.id} gpio1: {gpio1} gpio2: {gpio2} gpio3: {gpio3} gpio4: {gpio4} and gpio5: {gpio5}') 
                                        
                                    if self.config["TEMP_CONTROL"] == 'PID':
                                        inc = self.pid(current_temp2)
                                        self.time_to_on = round(inc)                        #소수점 첫번째에서 반올림
                                        self.pid_timer_call_time = 49                       #타이머 호출 회수 -1
                                        pid_t = Timer(0.1, self.pid_task).start()           #0.1초 타이머
                                    elif self.config["TEMP_CONTROL"] == 'TIMER':
                                        if ref_temp < current_temp2:
                                            self.time_to_on = int(self.config["TEMP_CONTROL_TIME"]) 
                                        else:
                                            self.time_to_on = 0
                                        self.pid_timer_call_time = 9  
                                        timer_t = Timer(1, self.timer_task).start()         #1초 타이머
                                        
                                    self.pid_timer_event.wait()                             #0.1초 또는 1초 타이머가 50번 또는 7 호출되는것을 기다림
                                    self.pid_timer_event.clear()
                                else:
                                    if self.writer_csv.closed:
                                        self.writer_csv.close()
                                    break
                        else:
                            self.timer_control_valve = False
                            if self.pid_timer_call_time > 0:
                                self.pid_timer_call_time = 0
                            #################################################
                            #message = {"UNIT_ID" : self.id,                  
                            #            "CMD":"TEMP_RPM",
                            #            "SPEED" : 0, 
                            #            "DIR"   : 'FW',            #FW = forward, RV = reverse
                            #            "ONOFF" : 'OFF', 
                            #            "TIME" : 0,
                            #            "SEND" : False}    
                            #self.command_queue.put(message) 
                            #self.logging.info(f"{message['CMD']} command is inserted Unit Board")
                            break
                    if not self.writer_csv.closed:
                        self.writer_csv.close()
                    self.pid.reset()            #pause 또는 stop이 오면 pid reset후 처음부터 다시 시작
                    self.set_cold_valve(OFF)    #pause 또는 stop이 오면 냉각 밸브를 off 시킴
                    ##############################################################################################################
                except Exception as e:
                    if self.writer_csv.closed:
                        self.writer_csv.close()
                    print(e)

class UnitBoard:
    def __init__(self, transmitte_queue, socket_send_queue, GPIOADDR1, GPIOADDR2, i2c_semaphor) -> None:
        self.can_fd_transmitte_queue = transmitte_queue
        self.socket_send_queue = socket_send_queue
        self.GPIOADDR1 = GPIOADDR1
        self.GPIOADDR2 = GPIOADDR2
        self.i2c_semaphor = i2c_semaphor
        # self.pid_update()
    
    # data 리스트에 있는 값들에 대해 CRC16 계산 후 data에 추가
    def crc16(self, data: list):
        crc = 0xFFFF
        for d in data:
            crc ^= d
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc

    def unit_process(self, n, shm, arr, semaphor, receive_queue, cmd_queue, logging):
        new_shm = shared_memory.SharedMemory(name=shm)
        shared_memory_u = np.ndarray(arr.shape, dtype=arr.dtype, buffer=new_shm.buf)
        logging.info(f'Process {os.getpid()} and {n} are created')  
        id = n
        unit_semaphor = semaphor
        can_fd_receive_queue = receive_queue
        command_queue = cmd_queue
        event = threading.Event()
        old_status = "None"
        old_stage = 1000
        try:
            self.config_file = configparser.ConfigParser()  ## 클래스 객체 생성
            self.config_file.read('/home/pi/Projects/cosmo-m/config/config.ini')  ## 파일 읽기        
            common_config = self.config_file['common']
            self.shared_memory_size = int(common_config['SHARED_MEMORY_SIZE'])
            self.config = self.config_file[f'unit_board{id}']
        except Exception as e:
            logging.error(f'id : {id} config.ini file open error')
            print(e)
        shared_memory_u[id * self.shared_memory_size] = os.getpid()   # id는 shared memory에 첫 번째 데이터  
        # 온도조절 관련 쓰레드 생성 ##################################################
        temp_thread = UnitBoardTempControl(id, event, logging, self.can_fd_transmitte_queue, 
                                                           command_queue, 
                                                           shared_memory_u, unit_semaphor, self.config, self.shared_memory_size)
        temp_thread.start()
        ######################################################################################################################################################
        # 처음 부팅이 되면 환경 설정을 유닛보드로 전송 ################################
        # SET_CONFIG 명령어 수행
        try:
            data = [i for i in range(7)]
            data[0] = 0x01
            data[1] = int(self.config['MOTOR_ID'], base=16)
            temp = int(self.config['SLEEP_SPEED'])
            data[2] = (temp >> 8) & 0xff        #big endian
            data[3] = temp & 0xff               #big endian
            data[4] = int(self.config['GPIO_INIT'], base=16) 
            data[5] = int(self.config['INVERTER'])
            # message의 data는 bytes형이 아니라면, int들의 list로 처리
            crc = self.crc16(data)
            # CRC16 2byte를 Little Endian으로 배열 뒤에 추가
            data.append(crc & 0xFF)
            data.append((crc >> 8) & 0xFF)
            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                    data=bytearray(data))
        except ValueError as e:
            print(e)
            
        while not can_fd_receive_queue.empty():
            can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
        
        self.can_fd_transmitte_queue.put(message) 
        time.sleep(0.10)

        if not can_fd_receive_queue.empty():
            logging.info(f'id : {id} unit board is initialized')    
            message = can_fd_receive_queue.get()
            if message.data[3] == 1:            # 1 : 정상, 0 : 오류
                fw_version = (message.data[1] << 8) | (message.data[2])
                logging.info(f'id : {id} firmware version is : {fw_version * 0.01 : 0.2F}')
            else:
                logging.warning(f'id : {id} unit board is wrong response')
        else:
            logging.warning(f'id : {id} unit board is not response')    
        ######################################################################################################################################################                        
        while True:
            try: 
                command = command_queue.get()
                
                if  command['CMD'] != 'GET_STATUS' and command['CMD'] != 'PING':         # GET_STATUS는 계속 호출 되므로 log에 출력 하지 않음.
                    if command['CMD'] == 'REF':
                        logging.info(f"{command['CMD']} command is inserted to {command['TANK_ID']} Unit Board")
                    elif command['CMD'] == 'STATE':
                        logging.info(f"{command['CMD']} command is inserted Unit Board")
                    elif command['CMD'] == 'TEMP_VALVE':
                        logging.info(f"{command['CMD']} and valve {command['VALUE']} command is inserted Unit Board")
                    elif command['CMD'] == 'WEIGHT_VALVE':
                        logging.info(f"{command['CMD']} and valve {command['VALUE']} command is inserted Unit Board")
                    elif command['CMD'] == 'TEMP_RPM':
                        logging.info(f"{command['CMD']} and rpm {command['SPEED']} command is inserted Unit Board")
                    else:
                        logging.info(f"{command['CMD']} command is inserted Unit Board")
                
                self.i2c_semaphor.acquire()
                i2cbus = smbus.SMBus(1)
                if int(command['UNIT_ID']) < 14:
                    i2cbus.write_byte_data(self.GPIOADDR1, 0x12, 0xFF & (~(int(command['UNIT_ID']) + 1)))
                else:
                    i2cbus.write_byte_data(self.GPIOADDR1, 0x13, 0xFF & (~(int(command['UNIT_ID']) + 1)))
                self.i2c_semaphor.release()
                if not command:
                    logging.warning(f'id : {id} Timeout waiting for command')
                else: 
                    if command['CMD'] == 'REF':
                        if int(self.config['TANK_ID']) == int(command['TANK_ID']) and int(self.config['ADDRESS'], 16) != 0xFFF:
                            temp_thread.ref_datas.append(command)
                    elif command['CMD'] == 'STATE':
                        if int(self.config['TANK_ID']) == int(command['DATA'][id]['TANK_ID']) and int(self.config['ADDRESS'], 16) != 0xFFF:
                            if command['DATA'][id]['STATUS'] == 'None':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                status = 0
                            elif command['DATA'][id]['STATUS'] == 'Stop':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                status = 1
                            elif command['DATA'][id]['STATUS'] == 'Run':
                                if not temp_thread.temp_control_start:      #기존 동작하지 않고 있으면 동작 함.
                                    if len(temp_thread.ref_datas) > 0:
                                        ref_command = temp_thread.ref_datas.pop(0)
                                        temp_thread.ref_stage = int(ref_command['STAGE'])
                                        temp_thread.ref_step = int(ref_command['STEP'])
                                        temp_thread.ref_data = ref_command['DATA']
                                        temp_thread.ref_total = len(temp_thread.ref_data)
                                    else:
                                        logging.error(f'id : {id} reference data is empty')
                                    temp_thread.temp_control_start = True
                                    temp_thread.file_write = True
                                    temp_thread.file_write_state = True
                                    temp_thread.file_index += 1
                                    event.set()
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                status = 2
                            elif command['DATA'][id]['STATUS'] == 'Pause':
                                if temp_thread.temp_control_start:      #기존 온도 쓰레드가 끝나지 않으면 강제 종료
                                    temp_thread.temp_control_start = False
                                    time.sleep(5)                       #PID 쓰레드는 5초 단위로 동작하므로 기존 쓰레드가 끝날 때 까지 5초 대기
                                if len(temp_thread.ref_datas) > 0:
                                    ref_command = temp_thread.ref_datas.pop(0)
                                    temp_thread.ref_stage = int(ref_command['STAGE'])
                                    temp_thread.ref_step = int(ref_command['STEP'])
                                    temp_thread.ref_data = ref_command['DATA']
                                    temp_thread.ref_total = len(temp_thread.ref_data)
                                    
                                    temp_thread.temp_control_start = True
                                    temp_thread.file_write_state = False
                                    event.set()
                                else:
                                    logging.error(f'id : {id} reference data is empty')
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                status = 3
                            elif command['DATA'][id]['STATUS'] == 'Initial':
                                temp_thread.temp_control_start = False
                                temp_thread.file_index = 0
                                temp_thread.ref_datas.clear()
                                temp_thread.dir_name = f"/home/pi/Projects/cosmo-m/data/{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                logging.info(f'id : {id} reference data status is  Initial')
                                status = 4
                            elif command['DATA'][id]['STATUS'] == 'Error':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | 0
                                status = 5
                            else:
                                status = 10
                            shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['DATA'][id]['STAGE']) << 16 | status
                            
                            if command['DATA'][id]['STATUS'] == 'Run' and int(command['DATA'][id]['STAGE']) != old_stage:
                                shared_memory_u[0x17 + id*self.shared_memory_size] = 0
                            self.old_stage = int(command['DATA'][id]['STAGE'])   
                    elif command['CMD'] == 'GET_ADC':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            data = [0x03]
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                        data=bytearray(data))
                            
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                
                            self.can_fd_transmitte_queue.put(message) 
                            # time.sleep(0.06)
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                if message.data[0] == 0x03:
                                    logging.info(f'id : {id} Received message: {message}')
                                    unit_semaphor.acquire()
                                    shared_memory_u[0x01 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[1] << 8) | (np.int32)(message.data[2]))
                                    shared_memory_u[0x02 + id*self.shared_memory_size]  = (np.int32)((np.int32)(message.data[3] << 8) | (np.int32)(message.data[4]))
                                    shared_memory_u[0x03 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[5] << 8) | (np.int32)(message.data[6]))
                                    shared_memory_u[0x04 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[7] << 8) | (np.int32)(message.data[8]))
                                    shared_memory_u[0x05 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[9] << 8) | (np.int32)(message.data[10]))
                                    shared_memory_u[0x06 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[11] << 8) | (np.int32)(message.data[12]))
                                    shared_memory_u[0x07 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[13] << 8) | (np.int32)(message.data[14]))
                                    shared_memory_u[0x08 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[15] << 8) | (np.int32)(message.data[16]))
                                    unit_semaphor.release()
                                    if command['SEND'] and self.socket_send_queue:
                                        self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!", 
                                                    "ADC1": f'{shared_memory_u[0x01 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC2": f'{shared_memory_u[0x02 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC3": f'{shared_memory_u[0x03 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC4": f'{shared_memory_u[0x04 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC5": f'{shared_memory_u[0x05 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC6": f'{shared_memory_u[0x06 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC7": f'{shared_memory_u[0x07 + id*self.shared_memory_size] * 0.001}',
                                                    "ADC8": f'{shared_memory_u[0x08 + id*self.shared_memory_size] * 0.001}'}), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response') 
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response') 
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'SET_GPIO':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            data = [0x05]
                            value = 0
                            for i in range(len(command['VALUE'])):
                                temp = 0 if command['VALUE'][i] == False else 1
                                value |= temp << i
                            data.append(value)
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                        data=bytearray(data))
                            
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                
                            self.can_fd_transmitte_queue.put(message) 
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                if message.data[0] == 0x05:
                                    logging.info(f'id : {id} Received message: {message}')
                                    if command['SEND'] and self.socket_send_queue:
                                        if message.data[1] == 1:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                                        else:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response') 
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response') 
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'GET_STATUS':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            # 온도계산 전에 GET_ADC를 호출 함. --> 명령어를 통합하여 ADC 값까지 GET_STATUS에서 읽어 옴.
                            data = [0x02]
                            crc = self.crc16(data)
                            # CRC16 2byte를 Little Endian으로 배열 뒤에 추가
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                        data=bytearray(data))   # message.data가 최대 길이 넘지 않게 조정 (CAN FD 사용시 유동적일 수 있음)

                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                            
                            self.can_fd_transmitte_queue.put(message) 
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break                           
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                
                                if message.data[0] == 0x02:
                                    if command['SEND']:                      # 로고에 너무 많이 쌓이는 데이터 방지, 
                                        logging.info(f'id : {id} Received message: {message}')
                                        
                                    unit_semaphor.acquire()

                                    I_mA = (np.float32)(message.data[1] << 8 | message.data[2])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                  
                                    shared_memory_u[0x0B + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[3] << 8 | message.data[4])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                   
                                    shared_memory_u[0x0C + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[5] << 8 | message.data[6])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                   
                                    shared_memory_u[0x0D + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[7] << 8 | message.data[8])*0.001
                                    T = -10 + (I_mA - 4) * (110) / 16                  
                                    shared_memory_u[0x0E + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[9] << 8 | message.data[10])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                    
                                    shared_memory_u[0x0F + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[11] << 8 | message.data[12])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                             
                                    shared_memory_u[0x10 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[13] << 8 | message.data[14])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                   
                                    shared_memory_u[0x11 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                                    I_mA = (np.float32)(message.data[15] << 8 | message.data[16])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                                    T = -10 + (I_mA - 4) * (110) / 16                   #-10 ~ 100 C -> 4mA ~ 20mA  유닛보드에서 * 1000이 되므로 여기서 4->4000, 16->16
                                    shared_memory_u[0x12 + id*self.shared_memory_size] = (np.int32)(T * 1000)
                                    
                                    if int(self.config['TANK_TYPE']) == 1 or int(self.config['TANK_TYPE']) == 2: #발효, 제성
                                        rs485_1 = (np.int32)(message.data[17] << 8 | message.data[18])
                                        shared_memory_u[0x23 + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                        rs485_2 = (np.int32)(message.data[19] << 8 | message.data[20])
                                        shared_memory_u[0x24 + id*self.shared_memory_size] = rs485_2      #보드에 따라 load cell 또는 다른 센서 값
                                        rs485_3 = (np.int32)(message.data[21] << 8 | message.data[22])
                                        shared_memory_u[0x28 + id*self.shared_memory_size] = rs485_3      #보드에 따라 ph 또는 다른 센서 값
                                        rs485_4 = (np.int32)(message.data[23] << 8 | message.data[24])
                                        shared_memory_u[0x26 + id*self.shared_memory_size] = rs485_4      #보드에 따라 Co2 또는 다른 센서 값
                                        # rs232_1 = (np.float32)(message.data[25] << 8 | message.data[26]) * 0.001
                                        # shared_memory_u[0x27 + id*self.shared_memory_size] = (np.int32)(rs232_1)      #보드에 따라 Flower 또는 다른 센서 값
                                    elif int(self.config['TANK_TYPE']) == 3: #숙성
                                        rs485_1 = (np.int32)(message.data[17] << 8 | message.data[18])
                                        shared_memory_u[0x28 + id*self.shared_memory_size] = rs485_1       #보드에 따라 PH 또는 다른 센서 값
                                    elif int(self.config['TANK_TYPE']) == 4: #제품
                                        rs485_1 = (np.int32)(message.data[17] << 8 | message.data[18])
                                        shared_memory_u[0x23 + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                        rs485_2 = (np.int32)(message.data[19] << 8 | message.data[20])
                                        shared_memory_u[0x24 + id*self.shared_memory_size] = rs485_2      #보드에 따라 load cell 또는 다른 센서 값
                                        rs485_3 = (np.int32)(message.data[21] << 8 | message.data[22])
                                        shared_memory_u[0x28 + id*self.shared_memory_size] = rs485_3      #보드에 따라 ph 또는 다른 센서 값
                                    elif int(self.config['TANK_TYPE']) == 5: #냉각수
                                        pass
                                    elif int(self.config['TANK_TYPE']) == 6: #물
                                        rs485_1 = (np.int32)(message.data[17] << 8 | message.data[18])
                                        shared_memory_u[0x23 + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                        rs485_2 = (np.int32)(message.data[19] << 8 | message.data[20])
                                        shared_memory_u[0x24 + id*self.shared_memory_size] = rs485_2      #보드에 따라 load cell 또는 다른 센서 값
                                    elif int(self.config['TANK_TYPE']) == 7: #밑술
                                        pass
                                    elif int(self.config['TANK_TYPE']) == 8: #펌프  
                                        pass
                                    elif int(self.config['TANK_TYPE']) == 9: #기타
                                        rs485_1 = (np.int32)(message.data[17] << 8 | message.data[18])
                                        shared_memory_u[0x27 + id*self.shared_memory_size] = rs485_1       #보드에 따라 유량량 또는 다른 센서 값
                                        rs485_2 = (np.int32)(message.data[19] << 8 | message.data[20])
                                        shared_memory_u[0x28 + id*self.shared_memory_size] = rs485_2      #보드에 따라 brix 또는 다른 센서 값
                                    
                                    # ADC 값
                                    shared_memory_u[0x01 + id*self.shared_memory_size] = (np.int32)((message.data[1] << 8) | message.data[2])
                                    shared_memory_u[0x02 + id*self.shared_memory_size] = (np.int32)((message.data[3] << 8) | message.data[4])
                                    shared_memory_u[0x03 + id*self.shared_memory_size] = (np.int32)((message.data[5] << 8) | message.data[6])
                                    shared_memory_u[0x04 + id*self.shared_memory_size] = (np.int32)((message.data[7] << 8) | message.data[8])
                                    shared_memory_u[0x05 + id*self.shared_memory_size] = (np.int32)((message.data[9] << 8) | message.data[10])
                                    shared_memory_u[0x06 + id*self.shared_memory_size] = (np.int32)((message.data[11] << 8) | message.data[12])
                                    shared_memory_u[0x07 + id*self.shared_memory_size] = (np.int32)((message.data[13] << 8) | message.data[14])
                                    shared_memory_u[0x08 + id*self.shared_memory_size] = (np.int32)((message.data[15] << 8) | message.data[16])

                                    # ADC 값에 온도 계산식을 추가해서 공유메모리에 저장 여기서는 2개만 계산하고 필요하면 추가.
                                    # shared_memory_u[0x10 + id*self.shared_memory_size] = float(f'{(inclination1 * shared_memory_u[0 + id*self.shared_memory_size] - y_offset1) * 100 : 0.2F}')
                                    # shared_memory_u[0x11 + id*self.shared_memory_size] = float(f'{(inclination2 * shared_memory_u[1 + id*self.shared_memory_size] - y_offset2) * 100 : 0.2F}')
                                    
                                    # shared_memory_u[0x12 + id*self.shared_memory_size] = float(f'{(inclination3 * shared_memory_u[2 + id*self.shared_memory_size] - y_offset3) * 100 : 0.2F}')
                                    # shared_memory_u[0x13 + id*self.shared_memory_size] = float(f'{(inclination4 * shared_memory_u[3 + id*self.shared_memory_size] - y_offset4) * 100 : 0.2F}')
                                    
                                    # shared_memory_u[0x14 + id*self.shared_memory_size] = float(f'{(inclination5 * shared_memory_u[4 + id*self.shared_memory_size] - y_offset5) * 100 : 0.2F}')
                                    # shared_memory_u[0x15 + id*self.shared_memory_size] = float(f'{(inclination6 * shared_memory_u[5 + id*self.shared_memory_size] - y_offset6) * 100 : 0.2F}')
                                    shared_memory_u[0x1A + id*self.shared_memory_size] = (np.int32)(message.data[27])     #GPO 7~0
                                    shared_memory_u[0x1C + id*self.shared_memory_size] = (np.int32)(message.data[28])    #GPI 7~0

                                    shared_memory_u[0x1E + id*self.shared_memory_size] = (np.int32)(message.data[29])    #inverter 상태
                                    # shared_memory_u[0x1F + id*self.shared_memory_size] = (np.int32)(message.data[30])    #inverter 상태
                                    unit_semaphor.release()
                                    
                                    # print(f'top temp. is {shared_memory_u[0x10 + id*self.shared_memory_size]*0.01:0.2F} and bottom temp. is {shared_memory_u[0x11 + id*self.shared_memory_size]*0.01}')
                                    # shared_memory_u[0x12 + id*self.shared_memory_size] = float(f'{(inclination3 * shared_memory_u[2 + id*self.shared_memory_size] - y_offset3) * 100 : 0.2F}')
                                    # shared_memory_u[0x13 + id*self.shared_memory_size] = float(f'{(inclination4 * shared_memory_u[3 + id*self.shared_memory_size] - y_offset4) * 100 : 0.2F}')
                                        
                                    if command['SEND'] and self.socket_send_queue:  # SEND는 자체 jsonserver_send.py에서 처리하므로 여기서는 처리하지 않음.
                                        self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!", 
                                            "TEMP1" : f'{shared_memory_u[0x0B + id*self.shared_memory_size] * 0.001: 0.2F}',
                                            "TEMP2" : f'{shared_memory_u[0x0C + id*self.shared_memory_size] * 0.001: 0.2F}',
                                            "GPO7~GPO0": f'{shared_memory_u[0x1A + id*self.shared_memory_size]}',
                                            "GPI7~GPI0": f'{shared_memory_u[0x1C + id*self.shared_memory_size]}',
                                            "RPM": f'{shared_memory_u[0x23 + id*self.shared_memory_size]}',
                                            "LOAD CELL": f'{shared_memory_u[0x24 + id*self.shared_memory_size] * 0.001 : 0.2F}kg',
                                            "SENSOR1": f'{shared_memory_u[0x25 + id*self.shared_memory_size] * 0.001 : 0.2F}',
                                            "SENSOR2": f'{shared_memory_u[0x26 + id*self.shared_memory_size] * 0.001 : 0.2F}%',
                                            "SENSOR3": f'{shared_memory_u[0x27 + id*self.shared_memory_size] * 0.001 : 0.2F}%',
                                            "SENSOR4": f'{shared_memory_u[0x28 + id*self.shared_memory_size] * 0.001 : 0.2F}%'
                                            }), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response')
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response') 
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'START_TEMP':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            event.set()
                            temp_thread.temp_control_start = True
                            
                            if command['SEND'] and self.socket_send_queue:
                                self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'STOP_TEMP':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            temp_thread.temp_control_start = False
                            
                            if command['SEND'] and self.socket_send_queue:
                                self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'TEMP_RPM':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            data = [0x07]
                            temp = int(command['SPEED'])
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)               # big endian
                            
                            if command['DIR'] == 'FW':
                                data.append(1)
                            else:
                                data.append(0)
                            if command['ONOFF'] == 'ON':
                                data.append(1)
                            else:
                                data.append(0)
                                
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))
                            
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                            
                            self.can_fd_transmitte_queue.put(message) 
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break                           
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                if message.data[0] == 0x07:
                                    logging.info(f'id : {id} Received message: {message}')
                                    if command['SEND'] and self.socket_send_queue:
                                        if message.data[1] == 1:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":f"success!"}), 'UTF-8'), block=False)
                                        else:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response')  
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response')   
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'TEMP_VALVE':
                        if int(self.config['ADDRESS'], 16) != 0xFFF:
                            data = [0x08]
                            data.append(int(command['CHANNEL']))
                            data.append(int(command['VALUE']))
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))
                            
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()              # as docs say: Remove and return an item from the queue.
                            
                            self.can_fd_transmitte_queue.put(message) 
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                if message.data[0] == 0x08:
                                    logging.info(f'id : {id} Received message: {message}')
                                    # temp_thread.set_cold_valve(message.data[5])   # 지속적인 재전송을 원하면 주석 해제. 
                                    
                                    if command['SEND'] and self.socket_send_queue:
                                        if message.data[1] == 1:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":f"success!"}), 'UTF-8'), block=False)
                                        else:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response')  
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response')  
                            # logging.info(f'id : {id} UnitBoard execute {command["CMD"]}')   
                    elif command['CMD'] == 'WEIGHT_VALVE':
                        if int(self.config['ADDRESS'], base=16) != 0xFFF:
                            data = [0x09]
                            data.append(int(command['CHANNEL']))
                            data.append(int(command['VALUE']))
                            temp = command['WEIGHT']               # int(float(command['CTRL'][0]['PARAM1']) * 10) 
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)               # big endian
                            
                            data.append(int(command['ONTIME']))
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))
                                
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                            
                            self.can_fd_transmitte_queue.put(message) 
                            # time.sleep(0.40)
                            wait = 0
                            while can_fd_receive_queue.empty():
                                time.sleep(0.01)
                                wait += 1
                                if wait > 120:
                                    break
                            if not can_fd_receive_queue.empty():
                                message = can_fd_receive_queue.get()
                                if message.data[0] == 0x09:
                                    logging.info(f'id : {id} Received message: {message}')
                                    if message.data[1] == 1:
                                        self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":f"success!"}), 'UTF-8'), block=False)
                                    else:
                                        self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                                else:
                                    logging.warning(f'id : {id} {command["CMD"]} unit board is wrong response')  
                            else:
                                logging.warning(f'id : {id} {command["CMD"]} unit board is not response')   
                    elif command['CMD'] == 'CTRL':
                        if int(self.config['TANK_ID']) == int(command['TANK_ID']) and int(self.config['ADDRESS'], 16) != 0xFFF:
                            if command['CTRL'][0]['SENSOR_ID'] == '500':    #밸브는 4개 밸브 아이디는 500부터 시작 500-> 냉각
                                x = self.config["SOLVALVE2"]                #밸브 I/O 번호
                                if command['CTRL'][0]['PARAM0'] == 'ON':
                                    value = ON
                                else:
                                    value = OFF
                                message = {"UNIT_ID" : id,                  
                                            "CMD":"TEMP_VALVE",
                                            "CHANNEL": x,
                                            "VALUE" : value}
                                command_queue.put(message, block=False) 
                            elif command['CTRL'][0]['SENSOR_ID'] == '501':  #밸브는 4개 밸브 아이디는 500부터 시작 501-> 워터
                                x = self.config["SOLVALVE1"]                #밸브 I/O 번호
                                if command['CTRL'][0]['PARAM0'] == 'ON':
                                    value = ON
                                else:
                                    value = OFF
                                #
                                temp = int(float(command['CTRL'][0]['PARAM1']) * 10) 
                                ontime = int(self.config["WATER_VALVE_ON_TIME"])
                                message = {"UNIT_ID" : id,                  
                                            "CMD":"WEIGHT_VALVE",
                                            "CHANNEL": x,
                                            "VALUE" : value,
                                            "WEIGHT" : temp, 
                                            "ONTIME" : ontime}
                                command_queue.put(message, block=False)
                            elif command['CTRL'][0]['SENSOR_ID'] == '502':
                                pass
                            elif command['CTRL'][0]['SENSOR_ID'] == '503':
                                pass
                            elif command['CTRL'][0]['SENSOR_ID'] == '600':     #모터 1개 모터 아이디는 600부터 시작
                                rpm = int(command['CTRL'][0]['PARAM0'])
                                run_time = int(command['CTRL'][0]['PARAM1'])
                                message = {"UNIT_ID" : id,                  
                                        "CMD":"TEMP_RPM",
                                        "SPEED" : rpm, 
                                        "DIR"   : 'FW',            #FW = forward, RV = reverse
                                        "ONOFF" : 'ON', 
                                        "TIME" : run_time,
                                        "SEND" : True}    
                                command_queue.put(message, block=False)
                self.i2c_semaphor.acquire()
                i2cbus.write_byte_data(self.GPIOADDR1, 0x12, 0xFF)
                i2cbus.write_byte_data(self.GPIOADDR1, 0x13, 0xFF)
                # i2cbus.write_byte_data(self.GPIOADDR2, 0x12, 0xFF)
                # i2cbus.write_byte_data(self.GPIOADDR2, 0x13, 0xFF)
                i2cbus.close()
                self.i2c_semaphor.release()
            except Exception as e:
                print(e)
                i2cbus.close()
                self.i2c_semaphor.release()
                unit_semaphor.release()
                logging.error(f'id : {id} {e}')
                print(traceback.print_exc())
