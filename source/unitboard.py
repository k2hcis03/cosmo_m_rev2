#! /usr/bin/python3

from typing import Any
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
from constdefine import ConstDefine
import queue

setpoint = 15.0  # Target temperature
Kp = 1.0  # Proportional gain
Ki = 0.5  # Integral gain
Kd = 0.1  # Derivative gain

ON = 1
OFF = 0
# g_file_path = "./data/JSON_Ref_Stage101.txt"
#
class UnitBoardCanFdReceive(threading.Thread):
    def __init__(self, id, logging, can_fd_receive_queue, shared_memory_u, shared_memory_size, unit_semaphor, 
        config, socket_send_queue, status_control_queue, unit_board_instance, common_config):
        threading.Thread.__init__(self)
        self.daemon = True
        self.id = id                            # id는 0부터 시작
        self.logging = logging
        self.can_fd_receive_queue = can_fd_receive_queue
        self.shared_memory_u = shared_memory_u
        self.shared_memory_size = shared_memory_size    
        self.unit_semaphor = unit_semaphor
        self.config = config
        self.socket_send_queue = socket_send_queue
        self.status_control_queue = status_control_queue
        self.unit_board_instance = unit_board_instance  # UnitBoard 인스턴스 참조 저장
        self.common_config = common_config
        self.water_motor_on_time = 0.0  # 최신 시간 값을 저장하는 변수
        self.water_motor_on = False
        self.logging.info(f'id: {self.id} UnitBoard CanFdReceive Thread Run')  
              
    def run(self):
        while True:
            try:
                time.sleep(0.10)
                while message := self.can_fd_receive_queue.get():
                    id = message.arbitration_id - 0x100
                    if message.data[0] == ConstDefine.SET_CONFIG_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} unit board is initialized')   
                            fw_version = (message.data[2] << 8) | (message.data[3])
                            self.logging.info(f'id: {id} firmware version is : {fw_version * 0.01 : 0.2F}')

                            ack_msg = bytes(json.dumps({'CMD':'ACK_INITIALIZE',
                                                        'IDX': 1,           #여기서는 임의의 값
                                                        'FW_VERSION': fw_version,
                                                        'NOTE': 'OK'
                                                        }), 'UTF-8')
                            self.socket_send_queue.put(ack_msg, block=False)
                        else:
                            self.logging.warning(f'id: {id} unit board is wrong response')
                    elif message.data[0] == ConstDefine.GET_STATUS_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            # GET_STATUS_COMMAND 메시지는 디버깅 로그에 출력하지 않음.
                            # self.logging.info(f'id: {id} Received message: {message}')   
                                
                            self.unit_semaphor.acquire()

                            I_mA = (np.float32)(message.data[2] << 8 | message.data[3])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                  
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP1 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[4] << 8 | message.data[5])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                   
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP2 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[6] << 8 | message.data[7])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                   
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP3 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[8] << 8 | message.data[9])*0.001
                            T = -10 + (I_mA - 4) * (110) / 16                  
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP4 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[10] << 8 | message.data[11])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                    
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP5 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[12] << 8 | message.data[13])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                             
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP6 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[14] << 8 | message.data[15])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                   
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP7 + id*self.shared_memory_size] = (np.int32)(T * 1000)

                            I_mA = (np.float32)(message.data[16] << 8 | message.data[17])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            T = -10 + (I_mA - 4) * (110) / 16                   #-10 ~ 100 C -> 4mA ~ 20mA  유닛보드에서 * 1000이 되므로 여기서 4->4000, 16->16
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP8 + id*self.shared_memory_size] = (np.int32)(T * 1000)
                            
                            # 탱크 종류 1: 발효, 2: 제성, 3: 숙성, 4: 제품, 5: 냉각수, 6: 물, 7: 밑술, 8: 펌프, 9:기타

                            if int(self.config['TANK_TYPE']) == 1 or int(self.config['TANK_TYPE']) == 2: #발효, 제성
                                rs485_1 = (np.int32)(message.data[18] << 8 | message.data[19])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_RPM + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                rs485_2 = (np.int32)(message.data[20] << 8 | message.data[21])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_LOAD_CELL + id*self.shared_memory_size] = rs485_2 #보드에 따라 load cell 또는 다른 센서 값
                                rs485_3 = (np.int32)(message.data[22] << 8 | message.data[23])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH1 + id*self.shared_memory_size] = rs485_3      #보드에 따라 ph 또는 다른 센서 값
                                rs485_4 = (np.int32)(message.data[24] << 8 | message.data[25])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_CO2 + id*self.shared_memory_size] = rs485_4      #보드에 따라 Co2 또는 다른 센서 값
                                # rs232_1 = (np.float32)(message.data[25] << 8 | message.data[26]) * 0.001
                                # self.shared_memory_u[0x27 + id*self.shared_memory_size] = (np.int32)(rs232_1)      #보드에 따라 Flower 또는 다른 센서 값
                            elif int(self.config['TANK_TYPE']) == 3: #숙성
                                rs485_1 = (np.int32)(message.data[18] << 8 | message.data[19])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH1 + id*self.shared_memory_size] = rs485_1       #보드에 따라 PH1 또는 다른 센서 값
                                rs485_2 = (np.int32)(message.data[20] << 8 | message.data[21])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH2 + id*self.shared_memory_size] = rs485_2       #보드에 따라 PH2 또는 다른 센서 값
                                rs485_3 = (np.int32)(message.data[22] << 8 | message.data[23])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH3 + id*self.shared_memory_size] = rs485_3      #보드에 따라 PH3 또는 다른 센서 값
                                rs485_4 = (np.int32)(message.data[24] << 8 | message.data[25])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH4 + id*self.shared_memory_size] = rs485_4      #보드에 따라 PH4 또는 다른 센서 값 
                            elif int(self.config['TANK_TYPE']) == 4: #제품
                                rs485_1 = (np.int32)(message.data[18] << 8 | message.data[19])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_RPM + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                rs485_2 = (np.int32)(message.data[20] << 8 | message.data[21])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_LOAD_CELL + id*self.shared_memory_size] = rs485_2      #보드에 따라 load cell 또는 다른 센서 값
                                rs485_3 = (np.int32)(message.data[22] << 8 | message.data[23])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_PH1 + id*self.shared_memory_size] = rs485_3      #보드에 따라 ph 또는 다른 센서 값
                            elif int(self.config['TANK_TYPE']) == 5: #냉각수
                                pass
                            elif int(self.config['TANK_TYPE']) == 6: #물
                                rs485_1 = (np.int32)(message.data[18] << 8 | message.data[19])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_RPM + id*self.shared_memory_size] = rs485_1       #보드에 따라 RPM 또는 다른 센서 값
                                rs485_2 = (np.int32)(message.data[20] << 8 | message.data[21])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_LOAD_CELL + id*self.shared_memory_size] = rs485_2      #보드에 따라 load cell 또는 다른 센서 값
                            elif int(self.config['TANK_TYPE']) == 7: #밑술
                                pass
                            elif int(self.config['TANK_TYPE']) == 8: #펌프  
                                pass
                            elif int(self.config['TANK_TYPE']) == 9: #기타
                                rs485_1 = (np.int32)(message.data[18] << 8 | message.data[19])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_FLOWER + id*self.shared_memory_size] = rs485_1       #보드에 따라 유량량 또는 다른 센서 값
                                rs485_2 = (np.int32)(message.data[20] << 8 | message.data[21])
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_BRIX + id*self.shared_memory_size] = rs485_2      #보드에 따라 brix 또는 다른 센서 값
                            
                            # ADC 값
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC1 + id*self.shared_memory_size] = (np.float32)(message.data[2] << 8 | message.data[3])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC2 + id*self.shared_memory_size] = (np.float32)(message.data[4] << 8 | message.data[5])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC3 + id*self.shared_memory_size] = (np.float32)(message.data[6] << 8 | message.data[7])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC4 + id*self.shared_memory_size] = (np.float32)(message.data[8] << 8 | message.data[9])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC5 + id*self.shared_memory_size] = (np.float32)(message.data[10] << 8 | message.data[11])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC6 + id*self.shared_memory_size] = (np.float32)(message.data[12] << 8 | message.data[13])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC7 + id*self.shared_memory_size] = (np.float32)(message.data[14] << 8 | message.data[15])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC8 + id*self.shared_memory_size] = (np.float32)(message.data[16] << 8 | message.data[17])*0.001 # 0.001은 유닛보드에서 * 1000이 되므로 여기서 0.001로 나누어줌.
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7 + id*self.shared_memory_size] = (np.int32)(message.data[28])     #GPO 7~0
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_GPI0_7 + id*self.shared_memory_size] = (np.int32)(message.data[29])    #GPI 7~0
                            
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_INVERTER_STATUS + id*self.shared_memory_size] = (np.int32)(message.data[30])    #inverter 상태
                            self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ERROR_CODE + id*self.shared_memory_size] = (np.int32)(message.data[45])    #error code
                            
                            # 유닛보드에서 물탱크 무게가 되거나 시간이 되서 모터 상태 값을 변경하면 모터 제어 처리함. 3.0은 동작 시간 확보
                            if self.water_motor_on_time + 3.0 < time.time() and self.water_motor_on == True:
                                max_unitboard = int(self.common_config['MAXUNITBOARD'])
                                # 탱크 종류 0 = 사용하지 않음, 1: 발효, 2: 제성, 3: 숙성, 4: 제품, 5: 냉각수, 6: 물, 7: 밑술, 8: 펌프, 9:기타                      
                                data = self.shared_memory_u[max_unitboard * self.shared_memory_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR]
                                if (np.int32)(message.data[31]) == OFF:
                                    data = data & ~(1 << id)
                                    self.water_motor_on = False
                                    self.shared_memory_u[max_unitboard * self.shared_memory_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR] = data
                            # shared_memory_u[0x1F + id*self.shared_memory_size] = (np.int32)(message.data[30])    #inverter 상태
                            self.unit_semaphor.release()
                            
                            # print(f'top temp. is {shared_memory_u[0x10 + id*self.shared_memory_size]*0.01:0.2F} and bottom temp. is {shared_memory_u[0x11 + id*self.shared_memory_size]*0.01}')
                            # shared_memory_u[0x12 + id*self.shared_memory_size] = float(f'{(inclination3 * shared_memory_u[2 + id*self.shared_memory_size] - y_offset3) * 100 : 0.2F}')
                            # shared_memory_u[0x13 + id*self.shared_memory_size] = float(f'{(inclination4 * shared_memory_u[3 + id*self.shared_memory_size] - y_offset4) * 100 : 0.2F}')                           
                        else:
                            self.logging.warning(f'id: {id} unit board GET_STATUS_COMMAND is wrong response')
                    elif message.data[0] == ConstDefine.GET_ADC_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            if message.data[0] == 0x03:
                                self.logging.info(f'id: {id} Received message: {message}')
                                self.unit_semaphor.acquire()
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC1 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[1] << 8) | (np.int32)(message.data[2]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC2 + id*self.shared_memory_size]  = (np.int32)((np.int32)(message.data[3] << 8) | (np.int32)(message.data[4]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC3 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[5] << 8) | (np.int32)(message.data[6]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC4 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[7] << 8) | (np.int32)(message.data[8]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC5 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[9] << 8) | (np.int32)(message.data[10]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC6 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[11] << 8) | (np.int32)(message.data[12]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC7 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[13] << 8) | (np.int32)(message.data[14]))
                                self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC8 + id*self.shared_memory_size] = (np.int32)((np.int32)(message.data[15] << 8) | (np.int32)(message.data[16]))
                                self.unit_semaphor.release()
                        else:
                            self.logging.warning(f'id: {id} unit board GET_ADC_COMMAND is wrong response')
                    elif message.data[0] == ConstDefine.GET_GPIO_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} unit board is gpio is : {message.data[2]}')
                        else:
                            self.logging.warning(f'id: {id} unit board GET_GPIO_COMMAND is wrong response')
                    elif message.data[0] == ConstDefine.SET_GPIO_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: SET_GPIO_COMMAND')                                 
                        else:
                            # self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                            self.logging.warning(f'id: {id} unit board SET_GPIO_COMMAND is wrong response') 
                    elif message.data[0] == ConstDefine.SET_MOTOR_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: SET_MOTOR_COMMAND')  
                        else:
                            self.logging.warning(f'id: {id} unit board SET_MOTOR_COMMAND is wrong response')
                    elif message.data[0] == ConstDefine.TEMP_RPM_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: TEMP_RPM_COMMAND')  
                        else:
                            # self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                            self.logging.warning(f'id: {id} unit board TEMP_RPM_COMMAND is wrong response')
                    elif message.data[0] == ConstDefine.TEMP_VALVE_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: TEMP_VALVE_COMMAND')                               
                        else:
                            # self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                            self.logging.warning(f'id: {id} unit board TEMP_VALVE_COMMAND is wrong response') 
                    elif message.data[0] == ConstDefine.WEIGHT_VALVE_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: WEIGHT_VALVE_COMMAND')
                            # self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":f"success!"}), 'UTF-8'), block=False)
                        else:
                            self.logging.warning(f'id: {id} unit board WEIGHT_VALVE_COMMAND is wrong response') 
                    elif message.data[0] == ConstDefine.GET_VERSION_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: GET_VERSION_COMMAND')
                            if self.socket_send_queue:
                                fw_version = (message.data[2] << 8) | (message.data[3])
                                # 아래 명령어를 보내면 서버에서 버전 정보를 업데이트 함.
                                ack_msg = bytes(json.dumps({'CMD':'ACK_INITIALIZE',                 
                                                            'IDX': 1,           #여기서는 임의의 값
                                                            'FW_VERSION': fw_version,
                                                            'NOTE': 'OK'
                                                            }), 'UTF-8')
                                self.socket_send_queue.put(ack_msg, block=False)                                   
                        else:
                            fw_version = (message.data[2] << 8) | (message.data[3])
                            ack_msg = bytes(json.dumps({'CMD':'ACK_INITIALIZE',
                                                        'IDX': 1,           #여기서는 임의의 값
                                                        'FW_VERSION': fw_version,
                                                        'NOTE': 'FAIL'
                                                        }), 'UTF-8')
                            self.socket_send_queue.put(ack_msg, block=False)
                            self.logging.warning(f'id: {id} unit board GET_VERSION_COMMAND is wrong response') 
                    elif message.data[0] == ConstDefine.FIRMWARE_UPDATE_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: FIRMWARE_UPDATE_COMMAND')
                            if self.socket_send_queue:
                                self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                                self.logging.info(f'id: {id} Firmware update success!')     
                            if self.status_control_queue:
                                try:
                                    time.sleep(0.5)
                                    if self.unit_board_instance:
                                        self.unit_board_instance.unit_board_initialize(id, self.logging)
                                    self.status_control_queue.put({"cmd": "RESUME_TIMER", "unit_id": id}, timeout=1)
                                except queue.Full:
                                    self.logging.error(f'id: {id} status timer resume 큐가 가득 찼습니다.')
                                except Exception as e:
                                    self.logging.error(f'id: {id} status timer resume 요청 실패: {e}')    
                        else:
                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                            self.logging.warning(f'id: {id} Firmware update fail!')
                            self.logging.warning(f'id: {id} unit board FIRMWARE_UPDATE_COMMAND is wrong response') 
                    elif message.data[0] == ConstDefine.BOOT_UNIT_BOARD_COMMAND:
                        if message.data[1] == 1:            # 1 : 정상, 0 : 오류
                            self.logging.info(f'id: {id} Received message: BOOT_UNIT_BOARD_COMMAND')
                            if self.socket_send_queue:
                                fw_version = (message.data[2] << 8) | (message.data[3])
                                # 아래 명령어를 보내면 서버에서 버전 정보를 업데이트 함.
                                ack_msg = bytes(json.dumps({'CMD':'ACK_INITIALIZE',                 
                                                            'IDX': 1,           #여기서는 임의의 값
                                                            'FW_VERSION': fw_version,
                                                            'NOTE': 'OK'
                                                            }), 'UTF-8')
                                self.socket_send_queue.put(ack_msg, block=False)                                   
                        else:
                            fw_version = (message.data[2] << 8) | (message.data[3])
                            ack_msg = bytes(json.dumps({'CMD':'ACK_INITIALIZE',
                                                        'IDX': 1,           #여기서는 임의의 값
                                                        'FW_VERSION': fw_version,
                                                        'NOTE': 'FAIL'
                                                        }), 'UTF-8')
                            self.socket_send_queue.put(ack_msg, block=False)
                            self.logging.warning(f'id: {id} unit board BOOT_UNIT_BOARD_COMMAND is wrong response') 
                        self.unit_board_instance.unit_board_initialize(id, self.logging)
                else:
                    self.logging.warning(f'id: {id} unit board is not response')    
            except Exception as e:
                print(e)
# 물탱크 유닛보드 일때, 생성되는 쓰레드 #########################
class UnitBoardWaterWeight(threading.Thread):
    def __init__(self, id, logging,  shared_memory_u, shared_memory_size, unit_semaphor, config, common_config, command_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.id = id                            # id는 0부터 시작
        self.logging = logging
        self.shared_memory_u = shared_memory_u
        self.shared_memory_size = shared_memory_size    
        self.unit_semaphor = unit_semaphor
        self.config = config
        self.common_config = common_config
        self.command_queue = command_queue
        self.logging.info(f'id: {self.id} UnitBoard WaterWeight Thread Run')
        self.gpio_value = [False, False, False, False, False, False, False, False]
        self.gpio_value_old = [False, False, False, False, False, False, False, False]

    def run(self):
        while True:
            try:
                time.sleep(0.5)  # 0.5초마다 
                
                max_unitboard = int(self.common_config['MAXUNITBOARD'])
                # 공유 메모리에서 물탱크 모터 상태 값 읽기
                self.unit_semaphor.acquire()
                water_motor = self.shared_memory_u[max_unitboard * self.shared_memory_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR]
                chiller1_motor = self.shared_memory_u[max_unitboard * self.shared_memory_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER1_MOTOR]
                chiller2_motor = self.shared_memory_u[max_unitboard * self.shared_memory_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER2_MOTOR]
                self.unit_semaphor.release()
                # 물탱크 모터 제어 명령어 전송
                if water_motor:
                    self.logging.info(f'id: {self.id} water motor is on')
                    self.gpio_value[int(self.config['SOLVALVE1'])] = True
                else:
                    self.gpio_value[int(self.config['SOLVALVE1'])] = False
                
                # 칠러1 모터 제어 명령어 전송
                if chiller1_motor:
                    self.logging.info(f'id: {self.id} chiller1 motor is on')   
                    self.gpio_value[int(self.config['SOLVALVE2'])] = True
                else:
                    self.gpio_value[int(self.config['SOLVALVE2'])] = False

                # 칠러2 모터 제어 명령어 전송   
                if chiller2_motor:
                    self.logging.info(f'id: {self.id} chiller2 motor is on')
                    # 칠러2 모터 제어 명령어 전송
                    self.gpio_value[int(self.config['SOLVALVE3'])] = True
                else:
                    self.gpio_value[int(self.config['SOLVALVE3'])] = False

                if self.gpio_value != self.gpio_value_old:
                    message = {"UNIT_ID" : self.id,                  
                    "CMD":"SET_GPIO",
                    "VALUE" : self.gpio_value}
                    self.command_queue.put(message, block=False) 
                self.gpio_value_old = self.gpio_value.copy()
            except Exception as e:
                self.logging.error(f'id: {self.id} UnitBoardWaterWeight Thread Error: {e}')
                print(e)

#                    
class UnitBoardTempControl(threading.Thread):
    def __init__(self, id, event, logging, can_fd_transmitte_queue, 
                 command_queue, shared_memory, unit_semaphor, config, shared_memory_size, dir_name, common_config):
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
        self.cold_valve_on_time = 0                 # Valve 릴레이가 ON되는 시간 변수
        self.pid_timer_call_time = 0        # pid or timer 계산 시간 5초. 0.1초 단위이므로 50
        self.unit_semaphor = unit_semaphor
        
        self.config = config
        self.dir_name = dir_name
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
        self.file_index = 0                 # CSV 파일 만들때 파일 번호
        self.writer_csv = None              # CSV 파일 핸들
        self.cold_valve_control_timer = False    # 타이머로 제어 할 때, 시간 마다 한 번만 제어 하기위한 변수
        self.motor_rpm = 0                  # 모터 현재 속도
        self.ref_continue = False          # 기존 reference 데이터를 사용할지 새로운 reference 데이터를 사용할지 결정
        self.ref_index = 0                  # reference 데이터 인덱스
        self.ref_file_name = None           # reference 데이터 파일 이름
        self.dir_data_name = None           # data 디렉토리 이름
        self.common_config = common_config
    
    def set_cold_valve(self, value):
        self.cold_valve_status = value
        x = self.config["SOLVALVE2"]        #냉각수 밸브 I/O 번호
        message = {"UNIT_ID" : self.id,                  
                    "CMD":"TEMP_VALVE",
                    "CHANNEL": x,
                    "VALUE" : value}
        self.command_queue.put(message, block=False) 
    
    def pid_task(self):   # ref온도가 현재 온도보다 높으면 아래기능 수행 
        if self.pid_timer_call_time > 0 and self.temp_control_start:            
            if self.cold_valve_on_time:
                self.cold_valve_on_time -= 1
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
            
    def timer_task(self):    # ref온도가 현재 온도보다 높으면 아래기능 수행
        if self.pid_timer_call_time > 0 and self.temp_control_start:            
            if self.cold_valve_on_time > 0 and self.cold_valve_control_timer:        # self.time_to_on시간 동안 valve를 ON 시킴 모터 속도가 100이하 또는 cold valve가 0일 때만 모터 명령어 전송
                self.cold_valve_on_time -= 1
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
                    self.cold_valve_control_timer = False
            self.pid_timer_call_time -= 1
            Timer(1, self.timer_task).start()
        else:
            Timer(1, self.timer_task).cancel()
            
            if self.cold_valve_status == 1:
                self.set_cold_valve(OFF) 
            # 한시간 마다 온도를 측정해서 냉각수를 구동시키기 때문에 위 if 문이 참이 아니면 다음 한시간을 기다리기 위해 아래
            # self.cold_valve_control_timer = False를 수행함
            self.cold_valve_control_timer = False
            self.pid_timer_event.set()
              
    def run(self):
            self.logging.info(f'id: {self.id} UnitBoard Temp Control Thread Run')
            while True:
                try:
                    # ref.json 파일이 존재하면 기존 reference 데이터를 사용하고 없으면 새로운 reference 데이터를 사용합니다.
                    self.ref_file_name = self.dir_name+'/ref.json'
                    if os.path.exists(self.ref_file_name) and int(self.common_config['REF_RESTART']) == 1:
                        self.logging.info(f"id: {self.id} : {self.ref_file_name} file is exist 기존 refernce로 진행합니다.")
                        self.ref_continue = True
                        try:
                            with open(self.ref_file_name, 'r', encoding='utf-8') as f:
                                self.ref_datas = json.load(f)
                            
                            self.logging.info(f'id: {self.id} ref_datas을 읽어왔습니다.')
                            self.ref_stage = int(self.ref_datas[0]['STAGE'])
                            self.ref_step = int(self.ref_datas[0]['STEP'])
                            self.ref_data = self.ref_datas[0]['DATA']
                            self.ref_total = len(self.ref_data)

                            self.temp_control_start = True
                            self.file_write_state = True
                            self.file_index  = 1
                            self.file_write = False
                        except Exception as e:
                            self.logging.error(f'id: {id} 기존 ref_datas 읽어오는 중 오류 발생: {e}')
                    else:
                        self.logging.info(f"id: {id} : {self.ref_file_name} file is not exist 새로운 refernce로 진행합니다.")
                        self.ref_continue = False
                    
                    self.ref_file_index = self.dir_name+'/ref.index'
                    if self.ref_continue:           #event 기다림 없이 그냥 진행
                        try:
                            with open(self.ref_file_index, 'r', encoding='utf-8') as f:
                                self.ref_index = int(f.read())
                            self.logging.info(f"id: {id} : ref.index 파일을 읽어왔습니다. {self.ref_index}")
                        except Exception as e:
                            self.ref_index = self.ref_total  #ref.index 파일이 없으면 for문 동작 시키지 않음
                            self.logging.error(f"id: {id} : ref.index 파일 읽기 중 오류 발생: {e}")
                    else:
                        self.ref_index = 0
                        self.event.wait()
                        self.event.clear()
                    # self.pid.reset() 테스트 필요
                    ##############################################################################################################
                    ## 온도 관련 동작   
                    ## 20230609
                    ## @K2H
                    ## 온도 테스트를 위한 데이저 저장. 
                    if self.file_write or self.ref_continue:
                        try:
                            if self.ref_continue:
                                try:
                                    with open(self.dir_name+'/csv.txt', 'r', encoding='utf-8') as f:
                                        csv_file_name = f.read().strip()
                                        self.dir_data_name = csv_file_name
                                        self.writer_csv = open(f'{self.dir_data_name}/pid_process{self.id}_{self.file_index}.csv', 'a', encoding='utf-8', newline='')
                                        self.writer = csv.writer(self.writer_csv, delimiter=',')
                                except Exception as e:
                                    self.logging.error(f"id: {self.id} : csv.txt 파일 읽기 중 오류 발생: {e}")   
                                    self.dir_data_name = f'/home/pi/Projects/cosmo-m/data/pid_process{self.id}_{self.file_index}.csv'  
                                                                    
                                    if not os.path.isdir(self.dir_data_name):
                                        os.makedirs(self.dir_data_name, exist_ok=True)
                                    self.writer_csv = open(f'{self.dir_data_name}/pid_process{self.id}_{self.file_index}.csv', 'w', encoding='utf-8', newline='')
                                    self.writer = csv.writer(self.writer_csv, delimiter=',')
                                    self.writer.writerow(['time'] + ['ref.temp'] + ['current temp'] + ['valve on time'] + ['relay1_water'] + ['relay2_cold'] + 
                                                        ['relay3_res1'] + ['relay4_res2'] + ['rpm'] + ['analog1_up'] + ['analog3_res1'] + ['analog4_res2'] + 
                                                        ['analog5_res3'] +['analog6_res4'] + ['analog7_res5'] + ['analog8_res6']) 
                            else:
                                # self.ref_continue == False일 때 dir_data_name이 None일 수 있으므로 초기화 필요
                                if self.dir_data_name is None:
                                    self.dir_data_name = f"/home/pi/Projects/cosmo-m/data/{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
                                if not os.path.isdir(self.dir_data_name):
                                    os.makedirs(self.dir_data_name, exist_ok=True)
                                self.writer_csv = open(f'{self.dir_data_name}/pid_process{self.id}_{self.file_index}.csv', 'w', encoding='utf-8', newline='')
                                self.writer = csv.writer(self.writer_csv, delimiter=',')
                                self.writer.writerow(['time'] + ['ref.temp'] + ['current temp'] + ['valve on time'] + ['relay1_water'] + ['relay2_cold'] + 
                                                    ['relay3_res1'] + ['relay4_res2'] + ['rpm'] + ['analog1_up'] + ['analog3_res1'] + ['analog4_res2'] + 
                                                    ['analog5_res3'] +['analog6_res4'] + ['analog7_res5'] + ['analog8_res6'])   
                                
                                if int(self.common_config['REF_RESTART']) == 1:
                                    try:
                                        csv_file_name = f'{self.dir_data_name}/pid_process{self.id}_{self.file_index}.csv'
                                        with open(self.dir_name+'/csv.txt', 'w', encoding='utf-8') as f:
                                            f.write(csv_file_name)
                                    except Exception as e:
                                        self.logging.error(f"id: {self.id} : ref.index 파일 저장 중 오류 발생: {e}")
                            #real temp == analog2, 
                            self.file_write = False
                        except Exception as e:
                            print(e)
                    ##############################################################################################################
                    for x in range(self.ref_index, self.ref_total):
                        # self.ref_file_index = self.dir_name      
                        self.logging.info(f"id: {self.id}: ref 파일을 순서대로 읽어옵니다. {x+1}")
                        print(f'id: {self.id} : ref 파일을 순서대로 읽어옵니다. {x+1 }')
                        try:
                            with open(self.ref_file_index, 'w', encoding='utf-8') as f:
                                f.write(str(x))
                        except Exception as e:
                            self.logging.error(f"id: {self.id} : ref.index 파일 저장 중 오류 발생: {e}")

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
                            # self.logging.info(f"id: {self.id} : {message['CMD']} command is inserted Unit Board")
                            # self.logging.info(f'id: {self.id} UnitBoard Temp Control Thread {x} Step Start at {time_start} Time')
                            self.cold_valve_control_timer = True     # ref_step마다 한번씩 ON 해준다.
                            
                            while (time_start + self.ref_step) > time.time():       #ref_step만큼 시간이 지나면 온도 제어 종료  self.pid_timer_call_time 시간 마다 호출출
                                if self.temp_control_start:
                                    # client.py에서 data = {"unit_id" : x , "cmd":"GET_STATUS", "send" : False, "raw" : False}
                                    # 로 데이터를 보내므로 온도 값이 계산되어 저장됨 따라서 *0.01을 하면 온도 값으로 사용    
                                    analog1 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP1 + self.id*self.shared_memory_size] * 0.001 #온도 센서 1
                                    analog2 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP2 + self.id*self.shared_memory_size] * 0.001 #온도 센서 2 온도 제어에 사용하는 하단 온도센서 
                                    analog3 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP3 + self.id*self.shared_memory_size] * 0.001 #온도 센서 3
                                    analog4 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP4 + self.id*self.shared_memory_size] * 0.001 #온도 센서 4
                                    analog5 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP5 + self.id*self.shared_memory_size] * 0.001 #온도 센서 5
                                    analog6 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP6 + self.id*self.shared_memory_size] * 0.001 #온도 센서 6
                                    analog7 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP7 + self.id*self.shared_memory_size] * 0.001 #온도 센서 6
                                    analog8 = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP8 + self.id*self.shared_memory_size] * 0.001 #온도 센서 6

                                    # 남양주 버전부터는 외부 온/습도 사용 안함.
                                    # current_ext_temp1 = self.shared_memory_u[0x0C + self.id*self.shared_memory_size] * 0.1 #Ext Temp1
                                    # current_ext_humi1 = self.shared_memory_u[0x0D + self.id*self.shared_memory_size] * 0.1 #Ext Humi1
                                    # current_ext_temp2 = self.shared_memory_u[0x0E + self.id*self.shared_memory_size] * 0.1 #Ext Temp2
                                    # current_ext_humi2 = self.shared_memory_u[0x0F + self.id*self.shared_memory_size] * 0.1 #Ext Humi2
                                    self.motor_rpm = self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_RPM + self.id*self.shared_memory_size]          #RPM

                                    gpo0_7 = (self.shared_memory_u[ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7 + self.id*self.shared_memory_size]) & 0xFF
                                    gpio1 = (gpo0_7 >> 7) & 0x01
                                    gpio2 = (gpo0_7 >> 6) & 0x01
                                    gpio3 = (gpo0_7 >> 5) & 0x01
                                    gpio4 = (gpo0_7 >> 4) & 0x01
                                    gpio5 = (gpo0_7 >> 3) & 0x01
                                    gpio6 = (gpo0_7 >> 2) & 0x01
                                    gpio7 = (gpo0_7 >> 1) & 0x01
                                    gpio8 = (gpo0_7 >> 0) & 0x01

                                    if self.file_write_state:                               #STATE가 Run이면 True Pause면 False
                                        self.writer.writerow([round(time.time(), 2), round(ref_temp, 2), round(analog2, 2), round(self.cold_valve_on_time, 2), 
                                                              round(gpio1, 2), round(gpio2, 2), round(gpio3, 2), round(gpio4, 2), round(self.motor_rpm, 2), 
                                                              round(analog1, 2), round(analog3, 2), round(analog4, 2), round(analog5, 2), round(analog6, 2), 
                                                              round(analog7, 2), round(analog8, 2)])
                                        print(f'id: {self.id} period: {time.time()} time to on: {self.cold_valve_on_time} REF.TEMP: {ref_temp} and CURRENT.TEMP: {analog2:0.2F}')
                                        
                                        # print(f'id: {self.id} analog1: {analog1} analog3: {analog3} analog4: {analog4} analog5: {analog5} and analog6: {analog6}')
                                        # print(f'id: {self.id} gpio1: {gpio1} gpio2: {gpio2} gpio3: {gpio3} gpio4: {gpio4} and gpio5: {gpio5}') 
                                        
                                    if self.config["TEMP_CONTROL"] == 'PID':
                                        inc = self.pid(analog2)
                                        self.cold_valve_on_time = round(inc)                        #소수점 첫번째에서 반올림
                                        self.pid_timer_call_time = 49                       #타이머 호출 회수 -1
                                        pid_t = Timer(0.1, self.pid_task).start()           #0.1초 타이머
                                    elif self.config["TEMP_CONTROL"] == 'TIMER':
                                        if ref_temp < analog2:
                                            self.cold_valve_on_time = int(self.config["TEMP_CONTROL_TIME"]) 
                                        else:
                                            self.cold_valve_on_time = 0
                                        self.pid_timer_call_time = 9  
                                        timer_t = Timer(1, self.timer_task).start()         #1초 타이머
                                        
                                    self.pid_timer_event.wait()                             #0.1초 또는 1초 타이머가 50번 또는 10 호출되는것을 기다림
                                    self.pid_timer_event.clear()
                                else:
                                    if self.writer_csv and not self.writer_csv.closed:
                                        self.writer_csv.close()
                                        self.writer_csv = None
                                    break
                        else:
                            self.cold_valve_control_timer = False
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
                            #self.logging.info(f"id: {self.id} : {message['CMD']} command is inserted Unit Board")
                            break
                    self.logging.info(f"id: {self.id}: ref 파일을 순서대로 읽어오는 동작이 종료되었습니다.")
                    print(f'id: {self.id}: ref 파일을 순서대로 읽어오는 동작이 종료되었습니다.')
                    if self.writer_csv and not self.writer_csv.closed:
                        self.writer_csv.close()
                        self.writer_csv = None
                    
                    ref_file_name = self.dir_name+'/ref.json'       #시컨스가 끝나면 ref.json 파일을 삭제
                    try:
                        if os.path.exists(ref_file_name):
                            os.remove(ref_file_name)
                            self.logging.info(f"id: {self.id} : {ref_file_name} file is removed")
                    except Exception as e:
                        self.logging.error(f"id: {self.id} : ref.json 파일 삭제 중 오류 발생: {e}")
                    
                    self.pid.reset()            #pause 또는 stop이 오면 pid reset후 처음부터 다시 시작
                    self.set_cold_valve(OFF)    #pause 또는 stop이 오면 냉각 밸브를 off 시킴
                    ##############################################################################################################
                except Exception as e:
                    if self.writer_csv and not self.writer_csv.closed:
                        self.writer_csv.close()
                        self.writer_csv = None
                    print(f"id: {self.id} : {e}")

class UnitBoard:
    def __init__(self, transmitte_queue, socket_send_queue, GPIOADDR1, GPIOADDR2, i2c_semaphor, status_control_queue) -> None:
        self.can_fd_transmitte_queue = transmitte_queue
        self.socket_send_queue = socket_send_queue
        self.GPIOADDR1 = GPIOADDR1
        self.GPIOADDR2 = GPIOADDR2
        self.i2c_semaphor = i2c_semaphor
        self.status_control_queue = status_control_queue
        # self.pid_update()
        self.dir_name = None                # data 기록 디렉토리 생성 ref data 저장 디렉토리
        self.motor_unit_board_gpo = 0

    def unit_board_initialize(self, id, logging):
        ######################################################################################################################################################
        # 처음 부팅이 되면 환경 설정을 유닛보드로 전송 ################################
        # SET_CONFIG 명령어 수행
        try:
            data = []
            data.append(ConstDefine.SET_CONFIG_COMMAND)
            data.append(int(self.config['MOTOR_ID'], base=16))
            temp = int(self.config['SLEEP_SPEED'])
            data.append((temp >> 8) & 0xff)        #big endian
            data.append(temp & 0xff)               #big endian
            data.append(int(self.config['GPIO_INIT'], base=16)) 
            data.append(int(self.config['RESERVED']))
            data.append(int(self.config['INVERTER_FREQUENCY']))
            temp = int(self.config['INVERTER_RPM'])
            data.append((temp >> 8) & 0xff)        #big endian
            data.append(temp & 0xff)               #big endian
            temp = int(self.config['INVERTER_MAX_HZ'])
            data.append((temp >> 8) & 0xff)        #big endian
            data.append(temp & 0xff)               #big endian
            data.append(int(self.config['INVERTER_ACC_DECEL_TIME']))
            data.append(int(self.config['MOTOR_POLE']))
            data.append(int(self.config['TANK_TYPE']))     
            data.append(int(self.config['RS485_1_USAGE']))
            data.append(int(self.config['RS485_2_USAGE']))
            data.append(int(self.config['RS485_3_USAGE']))
            data.append(int(self.config['RS485_4_USAGE']))
            data.append(int(self.config['RS232_1_USAGE']))
            data.append(int(self.config['TEMP_NUM']))
            temp = int(self.common_config['STATUS_TIME'])
            data.append((temp >> 8) & 0xff)        #big endian
            data.append(temp & 0xff)               #big endian
            data.append(int(self.common_config['UNIT_BOARD_ERROR_DETECTION']))
            # message의 data는 bytes형이 아니라면, int들의 list로 처리
            crc = self.crc16(data)
            # CRC16 2byte를 Little Endian으로 배열 뒤에 추가
            data.append(crc & 0xFF)
            data.append((crc >> 8) & 0xFF)
            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                    data=bytearray(data))
        except ValueError as e:
            print(e)
        
        self.can_fd_transmitte_queue.put(message) 
        ######################################################################################################################################################                        
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

    # WEIGHT_VALVE 타이머 콜백 함수  모터 구동 시간 후, 모터 정지 처리
    def timer_callback(self, id, shared_memory_u, unit_semaphor, can_fd_receive_thread):
        max_unitboard = int(self.common_config['MAXUNITBOARD'])
        shared_mem_size = int(self.common_config['SHARED_MEMORY_SIZE'])
        data = shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR]
        data = data & ~(1 << id)
        can_fd_receive_thread.water_motor_on = False
        unit_semaphor.acquire()
        shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR] = data
        unit_semaphor.release()

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
        self.dir_name = f"/home/pi/Projects/cosmo-m/ref/unit_board{id}"                # data 기록 디렉토리 생성 ref data 저장 디렉토리
       
        try:
            self.config_file = configparser.ConfigParser()  ## 클래스 객체 생성
            self.config_file.read('/home/pi/Projects/cosmo-m/config/config.ini')  ## 파일 읽기        
            self.common_config = self.config_file['common']
            self.shared_memory_size = int(self.common_config['SHARED_MEMORY_SIZE'])
            self.config = self.config_file[f'unit_board{id}']
        except Exception as e:
            logging.critical(f'id: {id} config.ini file open error - 프로세스를 종료합니다: {e}')
            print(e)
            return  # config 파일 없이는 동작 불가능하므로 프로세스 종료
        shared_memory_u[id * self.shared_memory_size] = os.getpid()   # id는 shared memory에 첫 번째 데이터  
        # 온도조절 관련 쓰레드 생성 ##################################################
        temp_thread = UnitBoardTempControl(id, event, logging, self.can_fd_transmitte_queue, 
                                                           command_queue, 
                                                           shared_memory_u, unit_semaphor, self.config, self.shared_memory_size, self.dir_name, self.common_config)
        temp_thread.start()
        while not can_fd_receive_queue.empty():
            can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.

        can_fd_receive_thread = UnitBoardCanFdReceive(id, logging, can_fd_receive_queue, shared_memory_u, self.shared_memory_size, unit_semaphor, 
            self.config, self.socket_send_queue, self.status_control_queue, self, self.common_config)
        can_fd_receive_thread.start()
        self.unit_board_initialize(id, logging)
        # 물탱크 유닛보드이면 물탱크 무게 측정 쓰레드 생성
        # 탱크 종류 0 = 사용하지 않음, 1: 발효, 2: 제성, 3: 숙성, 4: 제품, 5: 냉각수, 6: 물, 7: 밑술, 8: 펌프, 9:기타
        if int(self.config['TANK_TYPE']) == 6:
            water_weight_thread = UnitBoardWaterWeight(id, logging, shared_memory_u, self.shared_memory_size, unit_semaphor, 
            self.config, self.common_config, command_queue)
            water_weight_thread.start()
            logging.info(f'id: {id} UnitBoard WaterWeight Thread Run')
        while True:
            try: 
                global command
                command = command_queue.get()
                
                if  command['CMD'] != 'GET_STATUS' and command['CMD'] != 'PING':         # GET_STATUS는 계속 호출 되므로 log에 출력 하지 않음.
                    if command['CMD'] == 'REF':
                        logging.info(f"{command['CMD']} command is inserted to {id} Unit Board")
                    elif command['CMD'] == 'STATE':
                        logging.info(f"{command['CMD']} command is inserted to {id} Unit Board")
                    elif command['CMD'] == 'TEMP_VALVE':
                        logging.info(f"{command['CMD']} and valve status {command['VALUE']} command is inserted to {id} Unit Board")
                    elif command['CMD'] == 'WEIGHT_VALVE':
                        logging.info(f"{command['CMD']} and valve status {command['VALUE']} command is inserted to {id} Unit Board")
                    elif command['CMD'] == 'TEMP_RPM':
                        logging.info(f"{command['CMD']} and rpm speed {command['SPEED']} command is inserted to {id} Unit Board")
                    else:
                        logging.info(f"{command['CMD']} command is inserted to {id} Unit Board")
                
                self.i2c_semaphor.acquire()
                i2cbus = smbus.SMBus(1)
                try:
                    # led = int(self.config['ADDRESS']) 로 수정해야 됨. 아래 코드를 테스트 필요요
                    # led = int(command['UNIT_ID']) -1
                    led = int(self.config['ADDRESS'])
                    i2cbus.write_byte_data(self.GPIOADDR1, 0x12, 0xFF)
                    i2cbus.write_byte_data(self.GPIOADDR1, 0x13, 0xFF)
                    i2cbus.write_byte_data(self.GPIOADDR2, 0x12, 0xFF)
                    i2cbus.write_byte_data(self.GPIOADDR2, 0x13, 0xFF)
                    
                    if led >= 0:
                        if led <= 7:
                            led = led % 8
                            led = 1 << led
                            i2cbus.write_byte_data(self.GPIOADDR1, 0x12, 0xFF & (~led))
                        elif led > 7 and led <= 15:
                            led = led % 8
                            led = 1 << led
                            i2cbus.write_byte_data(self.GPIOADDR1, 0x13, 0xFF & (~led))
                        elif led > 15 and led <= 23:
                            led = led % 8
                            led = 1 << led
                            i2cbus.write_byte_data(self.GPIOADDR2, 0x12, 0xFF & (~led))
                        elif led > 23 and led <= 31:
                            led = led % 8
                            led = 1 << led
                            i2cbus.write_byte_data(self.GPIOADDR2, 0x13, 0xFF & (~led))
                except Exception as e:
                    logging.error(f'id: {id} I2C write error: {e}')
                finally:
                    self.i2c_semaphor.release()
                
                if not command:
                    logging.warning(f'id: {id} Timeout waiting for command')
                else: 
                    if command['CMD'] == 'REF':
                        if int(self.config['TANK_ID']) == int(command['TANK_ID']) and int(self.config['ADDRESS']) != 999:
                            temp_thread.ref_datas.append(command)
                            if not os.path.isdir(self.dir_name):    
                                os.makedirs(self.dir_name, exist_ok=True)
                            # temp_thread.ref_datas를 ref.json으로 저장합니다.
                            try:
                                # now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                                # ref_json_path = os.path.join(self.dir_name, f'{now_str}_ref.json')
                                # ref.json은 항상 마지막 데이터만 존재하도록 하고 메인 보드가 재부팅 등으로 다시 시작하면 마지막 데이터를 읽어오도록 함.
                                ref_json_path = os.path.join(self.dir_name, 'ref.json')
                                with open(ref_json_path, 'w', encoding='utf-8') as f:
                                    json.dump(temp_thread.ref_datas, f, ensure_ascii=False, indent=4)
                                logging.info(f'id: {id} ref_datas가 {ref_json_path}에 저장되었습니다.')
                            except Exception as e:
                                logging.error(f'id: {id} ref_datas를 ref.json으로 저장하는 중 오류 발생: {e}')
                            
                    elif command['CMD'] == 'STATE':
                        # STATE 문 수정해야 함.
                        if int(self.config['TANK_ID']) == int(command['TANK_ID']) and int(self.config['ADDRESS']) != 999:
                            unit_semaphor.acquire()
                            if command['STATUS'] == 'None':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                status = 0
                            elif command['STATUS'] == 'Stop':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                status = 1
                            elif command['STATUS'] == 'Run':
                                if not temp_thread.temp_control_start:      #기존 동작하지 않고 있으면 동작 함.
                                    if len(temp_thread.ref_datas) > 0:
                                        ref_command = temp_thread.ref_datas.pop(0)
                                        temp_thread.ref_stage = int(ref_command['STAGE'])
                                        temp_thread.ref_step = int(ref_command['STEP'])
                                        temp_thread.ref_data = ref_command['DATA']
                                        temp_thread.ref_total = len(temp_thread.ref_data)
                                    else:
                                        logging.error(f'id: {id} reference data is empty or restart')
                                    temp_thread.temp_control_start = True
                                    temp_thread.file_write = True
                                    temp_thread.file_write_state = True
                                    temp_thread.file_index  += 1
                                    event.set()
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                status = 2
                            elif command['STATUS'] == 'Pause':
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
                                    logging.error(f'id: {id} reference data is empty or pause')
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                status = 3
                            elif command['STATUS'] == 'Initial':
                                temp_thread.temp_control_start = False
                                temp_thread.file_index = 0
                                temp_thread.ref_datas.clear()
                                temp_thread.dir_data_name = f"/home/pi/Projects/cosmo-m/data/{datetime.datetime.now().strftime('%y%m%d_%H%M%S')}"
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                logging.info(f'id: {id} reference data status is  Initial')
                                status = 4
                            elif command['STATUS'] == 'Error':
                                temp_thread.temp_control_start = False
                                shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | 0
                                status = 5
                            else:
                                status = 10
                            shared_memory_u[0x18 + id*self.shared_memory_size] = int(command['STAGE']) << 16 | status
                            
                            if command['STATUS'] == 'Run' and int(command['STAGE']) != old_stage:
                                shared_memory_u[0x17 + id*self.shared_memory_size] = 0
                            old_stage = int(command['STAGE'])
                            unit_semaphor.release()
                    elif command['CMD'] == 'GET_ADC':
                        if int(self.config['ADDRESS']) != 999:
                            data = [ConstDefine.GET_ADC_COMMAND]
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                        data=bytearray(data))
                            
                            while not can_fd_receive_queue.empty():
                                can_fd_receive_queue.get()             # as docs say: Remove and return an item from the queue.
                
                            self.can_fd_transmitte_queue.put(message) 
                    elif command['CMD'] == 'SET_GPIO':
                        if int(self.config['ADDRESS']) != 999:
                            data = [ConstDefine.SET_GPIO_COMMAND]
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
                    elif command['CMD'] == 'GET_STATUS':    # 2025.12.09 - @K2H CAN FD 멀티 마스터 구현으로 여기 호출 안됨
                        pass
                    elif command['CMD'] == 'START_TEMP':
                        if int(self.config['ADDRESS']) != 999:
                            event.set()
                            temp_thread.temp_control_start = True
                            
                            if 'SEND' in command and command['SEND'] and self.socket_send_queue:
                                self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                            # logging.info(f'id: {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'STOP_TEMP':
                        if int(self.config['ADDRESS']) != 999:
                            temp_thread.temp_control_start = False
                            
                            if 'SEND' in command and command['SEND'] and self.socket_send_queue:
                                self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"success!"}), 'UTF-8'), block=False)
                            # logging.info(f'id: {id} UnitBoard execute {command["CMD"]}')
                    elif command['CMD'] == 'TEMP_RPM':
                        if int(self.config['ADDRESS']) != 999:
                            data = [ConstDefine.TEMP_RPM_COMMAND]
                            temp = int(command['SPEED'])
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)               # big endian
                            
                            temp = int(command['TIME']) 
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)  

                            if command['DIR'] == 'FW':
                                data.append(0)
                            else:
                                data.append(1)
                                
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))
                            self.can_fd_transmitte_queue.put(message) 
                    elif command['CMD'] == 'TEMP_VALVE':
                        if int(self.config['ADDRESS']) != 999:
                            # 물탱크 유닛보드에 칠러 모터 제어 명령어 전송 후, TEMP_VALVE 명령어 전송
                            max_unitboard = int(self.common_config['MAXUNITBOARD'])
                            shared_mem_size = int(self.common_config['SHARED_MEMORY_SIZE'])
                            unit_semaphor.acquire()
                            # 탱크 종류 0 = 사용하지 않음, 1: 발효, 2: 제성, 3: 숙성, 4: 제품, 5: 냉각수, 6: 물, 7: 밑술, 8: 펌프, 9:기타
                            if int(self.config['TANK_TYPE']) == 1:
                                data = shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER1_MOTOR]
                                if command['VALUE'] == ON:
                                    data = data | (1 << id)
                                else:
                                    data = data & ~(1 << id)
                                shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER1_MOTOR] = data
                            elif int(self.config['TANK_TYPE']) == 2:
                                data = shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER2_MOTOR]
                                if command['VALUE'] == ON:
                                    data = data | (1 << id)
                                else:
                                    data = data & ~(1 << id)
                                shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_CHILLER2_MOTOR] = data
                            unit_semaphor.release()
                            time.sleep(0.05)     #여유 시간
                            # 물탱크 유닛보드에 칠러 모터 제어 명령어 전송 끝
                                       
                            # TEMP_VALVE 명령어 전송
                            data = [ConstDefine.TEMP_VALVE_COMMAND]
                            data.append(int(command['CHANNEL']))
                            data.append(int(command['VALUE']))
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))
                            self.can_fd_transmitte_queue.put(message) 
                    elif command['CMD'] == 'WEIGHT_VALVE':
                        if int(self.config['ADDRESS']) != 999:
                            # 물탱크 유닛보드에 모터 제어 명령어 전송 후, WEIGHT_VALVE 명령어 전송
                            max_unitboard = int(self.common_config['MAXUNITBOARD'])
                            shared_mem_size = int(self.common_config['SHARED_MEMORY_SIZE'])
                            unit_semaphor.acquire()
                            # 탱크 종류 0 = 사용하지 않음, 1: 발효, 2: 제성, 3: 숙성, 4: 제품, 5: 냉각수, 6: 물, 7: 밑술, 8: 펌프, 9:기타                      
                            data = shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR]
                            
                            if command['VALUE'] == ON:
                                data = data | (1 << id)
                            else:
                                data = data & ~(1 << id)
                            
                            shared_memory_u[max_unitboard * shared_mem_size + ConstDefine.SHARED_MEMORY_OFFSET_WATER_MOTOR] = data
                            unit_semaphor.release()
                            time.sleep(0.05)     #여유 시간
                            # 물탱크 유닛보드에 모터 제어 명령어 전송 끝
                            
                            data = [ConstDefine.WEIGHT_VALVE_COMMAND]
                            data.append(int(command['CHANNEL']))
                            data.append(int(command['VALUE']))
                            temp = command['WEIGHT']               # int(float(command['CTRL'][0]['PARAM1']) * 10) 
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)               # big endian
                            
                            temp = int(command['ONTIME'])          # ONTIME은 초 단위로 전송
                            data.append((temp >> 8) & 0xff)        # big endian
                            data.append(temp & 0xff)               # big endian
                            
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, bitrate_switch = True, arbitration_id=id,  
                                        data=bytearray(data))                           
                            self.can_fd_transmitte_queue.put(message) 
                    elif command['CMD'] == 'CTRL':
                        if int(self.config['TANK_ID']) == int(command['TANK_ID']) and int(self.config['ADDRESS']) != 999:
                            if command['CTRL'][0]['SENSOR_ID'] == '1500':    #밸브는 4개 밸브 아이디는 500부터 시작 1500-> 냉각
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
                            elif command['CTRL'][0]['SENSOR_ID'] == '1501':  #밸브는 4개 밸브 아이디는 500부터 시작 1501-> 워터
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
                                current_time = time.time()
                                can_fd_receive_thread.water_motor_on_time = current_time  # UnitBoardCanFdReceive에 최신 시간 값 전달
                                can_fd_receive_thread.water_motor_on = True
                                Timer(ontime, self.timer_callback, args=(id, shared_memory_u, unit_semaphor, can_fd_receive_thread)).start()
                            elif command['CTRL'][0]['SENSOR_ID'] == '1502':
                                pass
                            elif command['CTRL'][0]['SENSOR_ID'] == '1503':
                                pass
                            elif command['CTRL'][0]['SENSOR_ID'] == '1600':     #모터 1개 모터 아이디는 600부터 시작
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
                    elif command['CMD'] == 'FIRMWARE_UPDATE':                      
                        if int(self.config['ADDRESS']) != 999:
                            if self.status_control_queue:
                                try:
                                    self.status_control_queue.put({"cmd": "PAUSE_TIMER", "unit_id": id}, timeout=1)
                                except queue.Full:
                                    logging.error(f'id: {id} status timer pause 큐가 가득 찼습니다.')
                                except Exception as e:
                                    logging.error(f'id: {id} status timer pause 요청 실패: {e}')
                            try:
                                data = [ConstDefine.FIRMWARE_UPDATE_COMMAND]
                                file_temp = []
                                is_last = False
                                with open(command['FILE'], 'rb') as f:
                                    while True:
                                        chunk = f.read(1024)
                                        if not chunk:
                                            break
                                        file_temp.extend(chunk)

                                if not file_temp:
                                    logging.error(f'id: {id} 펌웨어 파일이 비어 있습니다. path={command["FILE"]}')
                                    if 'SEND' in command and command['SEND'] and self.socket_send_queue:
                                        try:
                                            self.socket_send_queue.put(bytes(json.dumps({"id" : f'{id}', "status":"fail!"}), 'UTF-8'), block=False)
                                        except queue.Full:
                                            logging.error('socket_send_queue is full. 펌웨어 업데이트 실패 상태를 전송하지 못했습니다.')
                                    continue

                                offset = 0
                                index = 0
                                chunk = None
                                while offset < len(file_temp):
                                    chunk = file_temp[offset:offset+56]
                                    if len(chunk) < 56 or offset + 56 >= len(file_temp):
                                        is_last = 1
                                    else:
                                        is_last = 0
                                    
                                    data.append((index >> 8) & 0xff)        # big endian
                                    data.append(index & 0xff)     
                                    data.append(len(chunk))
                                    data.append(is_last)
                                    data = data + list(chunk)
                                    crc = self.crc16(data)
                                    data.append(crc & 0xFF)
                                    data.append((crc >> 8) & 0xFF)
                                    message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                                data=bytearray(data))
                                    # time.sleep(0.005)
                                    offset += 56
                                    print(f'index: {index}')
                                    index += 1
                                    
                                    if is_last:
                                        break
                                    else:
                                        data = [ConstDefine.FIRMWARE_UPDATE_COMMAND]
                                        self.can_fd_transmitte_queue.put(message) 
                                    time.sleep(0.002)                       # 0.002초 대기 없으면 파일 전송 실패 (디버그 모드에서는 동작하나 단독 실행시 실패)
                                self.can_fd_transmitte_queue.put(message) 
                            except Exception as e:
                                logging.error(f'id: {id} 펌웨어 업데이트 중 오류 발생: {e}')
                    elif command['CMD'] == 'GET_VERSION':
                        if int(self.config['ADDRESS']) != 999:
                            data = [ConstDefine.GET_VERSION_COMMAND]                 
                            crc = self.crc16(data)
                            data.append(crc & 0xFF)
                            data.append((crc >> 8) & 0xFF)
                            message = can.Message(is_extended_id=False, is_fd = True, arbitration_id=id, bitrate_switch = True,
                                        data=bytearray(data))
                            self.can_fd_transmitte_queue.put(message) 
                self.i2c_semaphor.acquire()
                i2cbus.write_byte_data(self.GPIOADDR1, 0x12, 0xFF)
                i2cbus.write_byte_data(self.GPIOADDR1, 0x13, 0xFF)
                i2cbus.write_byte_data(self.GPIOADDR2, 0x12, 0xFF)
                i2cbus.write_byte_data(self.GPIOADDR2, 0x13, 0xFF)
                i2cbus.close()
                self.i2c_semaphor.release()
            except Exception as e:
                print(e)
                i2cbus.close()
                self.i2c_semaphor.release()
                unit_semaphor.release()
                logging.error(f'id: {id} {e}')
                print(traceback.print_exc())