#! /usr/bin/python3

import socket
import json
import time
import threading
import smbus
import numpy as np
import queue

from threading import Timer
from multiprocessing import shared_memory
from multiprocessing.managers import RemoteError
import os
import traceback
import configparser
from signal import signal, SIGPIPE, SIG_DFL
from constdefine import ConstDefine
# signal(SIGPIPE,SIG_DFL)

class UnitBoardGetStatus(threading.Thread):
    def __init__(self, logging, tcp_queue, max_unit_board, shared_memory, socket_send_queue, receive_event, status_control_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.logging = logging
        self.tcp_queue = tcp_queue
        self.max_unit_board = max_unit_board
        self.shared_memory = shared_memory
        self.send_index = 0
        self.socket_send_queue = socket_send_queue
        self.client = None
        self.receive_event = receive_event
        self.timer_active = False
        self.timer_lock = threading.Lock()
        self.consecutive_errors = 0
        self.max_error_threshold = 5
        self.status_control_queue = status_control_queue
        self.pause_count = 0
        self.control_thread = None
        if self.status_control_queue is not None:
            self.control_thread = threading.Thread(target=self.control_loop, daemon=True)
            self.control_thread.start()
        
        # Config 파일 읽기 with 예외 처리
        self.config_file = configparser.ConfigParser()
        self.canfd_communication_error_cnt = [0 for _ in range(self.max_unit_board)]
        try:
            config_result = self.config_file.read('/home/pi/Projects/cosmo-m/config/config.ini')
            if not config_result:
                logging.error('Config file not found, using default values')
                # 기본값 설정
                self.config_file['common'] = {
                    'SHARED_MEMORY_SIZE': '50',
                    'FERMEN_TANK': '0',
                    'BLEND_TANK': '0',
                    'PROD_TANK': '0',
                    'CHILER_TANK': '0'
                }
        except Exception as e:
            logging.error(f'Failed to read config file: {e}, using default values')
            self.config_file['common'] = {
                'SHARED_MEMORY_SIZE': '50',
                'FERMEN_TANK': '0',
                'BLEND_TANK': '0',
                'PROD_TANK': '0',
                'CHILER_TANK': '0'
            }
    
        self.send_data = []
        self.order = 0
        try:
            self.make_json_data()
        except Exception as e:
            logging.error(f'Initial make_json_data failed: {e}')
            self.send_data = {"CMD": "SENSOR", "ORDER": "0", "VALUES": [], "STATE": []}
        
        logging.info(f'UnitBoardGetStatus thread initialized') 
        self.transmit_event = threading.Event()
        self.transmit_event.set() # 기본적으로 전송 가능 상태
        
    def control_loop(self):
        while True:
            try:
                message = self.status_control_queue.get(timeout=5.0)
                if not message:
                    continue
                command = message.get('cmd')
                unit_id = message.get('unit_id')
                if command == 'PAUSE_TIMER':
                    self.pause_status_timer(unit_id)
                elif command == 'RESUME_TIMER':
                    self.resume_status_timer(unit_id)
            except queue.Empty:
                # 타임아웃 발생 시 루프를 계속 진행 (정상 동작)
                continue
            except Exception as e:
                self.logging.error(f'Status timer control error: {e}')

    def pause_status_timer(self, unit_id):
        with self.timer_lock:
            self.pause_count += 1
            self.timer_active = False
            self.transmit_event.clear() # 전송 일시 중지
            if self.pause_count == 1:
                self.logging.info(f'펌웨어 업데이트 진행으로 상태 전송 타이머를 일시 중지합니다. (unit:{unit_id})')
            else:
                self.logging.debug(f'펌웨어 업데이트 대기 중 타이머 일시 중지 유지. count={self.pause_count} (unit:{unit_id})')

    def resume_status_timer(self, unit_id):
        with self.timer_lock:
            if self.pause_count == 0:
                self.logging.debug(f'펌웨어 업데이트 종료 신호가 도착했지만 대기 중인 pause가 없습니다. (unit:{unit_id})')
                if not self.timer_active:
                    self.timer_active = True
                    Timer(1, self.timer_update_task).start()
                return
            self.pause_count -= 1
            if self.pause_count == 0:
                self.transmit_event.set() # 전송 재개
                if not self.timer_active:
                    self.timer_active = True
                    Timer(1, self.timer_update_task).start()
                self.logging.info(f'펌웨어 업데이트가 완료되어 상태 전송 타이머를 재개합니다. (unit:{unit_id})')
            else:
                self.logging.debug(f'다른 펌웨어 업데이트가 진행 중이므로 타이머 재개를 대기합니다. 남은 count={self.pause_count} (unit:{unit_id})')
        
    def make_json_data(self):
        """JSON 데이터 생성 with 예외 처리"""
        try:
            common_config = self.config_file['common']
            size = int(common_config['SHARED_MEMORY_SIZE'])
      
            if self.order >= 1000000:
                self.order = 0
                
            self.send_data = {
                "CMD": "SENSOR",
                "ORDER": f"{self.order}",
                "DATE":f"{time.strftime('%Y-%m-%d', time.localtime(time.time()))}",
                "TIME":f"{time.strftime('%H:%M:%S')}",
                "VALUES":[],
                "STATE":[],
                "ERROR":[],
                # "CODE":{"CODE":1000,"MSG":"OK"}
            }
            temp_index = [ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP1, 
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP2,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP3,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP4,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP5,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP6,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP7,
                          ConstDefine.SHARED_MEMORY_OFFSET_ADC_TEMP8]           #온도 값이 저장되는 shared_memory 위치 
            humi_index = [0x13, 
                        0x15]                       #습도 값이 저장되는 shared_memory 위치
            co2_index = [ConstDefine.SHARED_MEMORY_OFFSET_CO2, 
                         ConstDefine.SHARED_MEMORY_OFFSET_CO2]                        #Co2 값이 저장되는 shared_memory 위치
            ph_index = [ConstDefine.SHARED_MEMORY_OFFSET_PH1, 
                        ConstDefine.SHARED_MEMORY_OFFSET_PH2,
                        ConstDefine.SHARED_MEMORY_OFFSET_PH3,
                        ConstDefine.SHARED_MEMORY_OFFSET_PH4]                         #PH 값이 저장되는 shared_memory 위치
            brix_index = [ConstDefine.SHARED_MEMORY_OFFSET_BRIX, 
                          ConstDefine.SHARED_MEMORY_OFFSET_BRIX]                       #Brix 값이 저장되는 shared_memory 위치
            flow_index = [ConstDefine.SHARED_MEMORY_OFFSET_FLOWER, 
                          ConstDefine.SHARED_MEMORY_OFFSET_FLOWER]                       #Flow 값이 저장되는 shared_memory 위치
            loadcell_index = [ConstDefine.SHARED_MEMORY_OFFSET_LOAD_CELL, 
                              ConstDefine.SHARED_MEMORY_OFFSET_LOAD_CELL]                   #Load Cell 값이 저장되는 shared_memory 위치
            gpo_index = [ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7, 
                         ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7,
                         ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7,
                         ConstDefine.SHARED_MEMORY_OFFSET_GPO0_7]                        #gpo 값이 저장되는 shared_memory 위치
            gpi_index = [ConstDefine.SHARED_MEMORY_OFFSET_GPI0_7, 
                         ConstDefine.SHARED_MEMORY_OFFSET_GPI0_7]                        #gpi 값이 저장되는 shared_memory 위치
            inverter_index = [ConstDefine.SHARED_MEMORY_OFFSET_INVERTER_STATUS, 
                              ConstDefine.SHARED_MEMORY_OFFSET_INVERTER_STATUS]                   #inverter 값이 저장되는 shared_memory 위치
            motor_index = [ConstDefine.SHARED_MEMORY_OFFSET_RPM, 
                           ConstDefine.SHARED_MEMORY_OFFSET_RPM]                      #motor 값이 저장되는 shared_memory 위치
            ct_index = [ConstDefine.SHARED_MEMORY_OFFSET_ADC1, 
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC2,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC3,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC4,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC5,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC6,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC7,
                        ConstDefine.SHARED_MEMORY_OFFSET_ADC8]           #CT 값이 저장되는 shared_memory 위치 
            error_index = [ConstDefine.SHARED_MEMORY_OFFSET_ERROR_CODE]           #Error 값이 저장되는 shared_memory 위치 
            self.order += 1

            for i in range(int(common_config['MAXUNITBOARD'])):
                try:
                    unit_config = self.config_file[f'unit_board{i}']
                    
                    if int(unit_config.get('ADDRESS', 0)) == 999:
                        continue
                    # Shared memory 인덱스 범위 검증
                    base_index = i * size
                    if base_index >= len(self.shared_memory):
                        self.logging.warning(f'Shared memory index out of range for FERMEN_TANK {i}')
                        continue
                    # config.ini에서 ADC usage 설정에 따라 온도 센서 또는 유량센서 전송 기능 필요. 모두 온도 센서로 값을 채우고 아래에서 유량센서, CT 전송 기능 추가.
                    x_index = 0
                    for x in range(int(unit_config.get('ADC_NUM', 0))):
                        if x < len(temp_index):
                            mem_idx = base_index + temp_index[x]
                            adc_usage = int(unit_config.get(f'ADC_{x+1}', 0))
                            if adc_usage == 1 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1100+x_index}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                                x_index += 1

                    for x in range(int(unit_config.get('HUMI_NUM', 0))):
                        if x < len(humi_index):
                            mem_idx = base_index + humi_index[x]
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1200+x}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('CO2_NUM', 0))):
                        if x < len(co2_index):
                            mem_idx = base_index + co2_index[x]
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1300+x}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('LOAD_CELL_NUM', 0))):
                        if x < len(loadcell_index):
                            mem_idx = base_index + loadcell_index[x]
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1400+x}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('VALVE_NUM', 0))):
                        if x < len(gpo_index):
                            if int(unit_config.get('ENCODER_USAGE', 0)) == 0:
                                mem_idx = base_index + gpo_index[0]     #gpo 값이 저장되는 shared_memory 위치는 VALVE_NUM 상관없이 1개
                                if mem_idx < len(self.shared_memory):
                                    self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1500+x}","VALUE":                   # 상단, 하단 솔밸브
                                        f"{(self.shared_memory[mem_idx] >> x) & 0x00000001}"})
                            else:
                                mem_idx = base_index + gpi_index[0]     #gpi 값이 저장되는 shared_memory 위치는 VALVE_NUM 상관없이 1개
                                if mem_idx < len(self.shared_memory):
                                    self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1500+x}","VALUE":                   # 상단, 하단 솔밸브
                                        f"{((self.shared_memory[mem_idx] >> x) ^ 0x00000001) & 0x00000001}"})

                    for x in range(int(unit_config.get('MOTOR_NUM', 0))): 
                        if x < len(motor_index):
                            mem_idx = base_index + motor_index[x]
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1600+x}","VALUE":f"{self.shared_memory[mem_idx]*1:0.2F}"}) # 모터 속도는 유닛보드에서 *1000을 안함(오버플로우우)
                  
                    for x in range(int(unit_config.get('BRIX_NUM', 0))):
                        if x < len(brix_index):
                            mem_idx = base_index + brix_index[x]          #brix 값이 저장되는 shared_memory 위치는 ADC_NUM과 상관없이 1개이기 때문에 x가 0이여도 됨.
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1700+x}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})

                    for x in range(int(unit_config.get('PH_NUM', 0))):
                        if x < len(ph_index):
                            mem_idx = base_index + ph_index[x]          #ph 값이 저장되는 shared_memory 위치는 ADC_NUM과 상관없이 1개이기 때문에 x가 0이여도 됨.
                            if mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{1800+x}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    # 유량센서 전송 기능
                    x_index = 0
                    for x in range(int(unit_config.get('ADC_NUM', 0))):
                        if x < len(flow_index):
                            mem_idx = base_index + flow_index[0]          #flow 값이 저장되는 shared_memory 위치는 ADC_NUM과 상관없이 1개
                            adc_usage = int(unit_config.get(f'ADC_{x+1}', 0))
                            if adc_usage == 2 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{2000+x_index}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                                x_index += 1

                    # CT 센서 전송 기능  센서 번호 김휴정 박사와 협의 필요 
                    for x in range(int(unit_config.get('ADC_NUM', 0))):
                        if x < len(ct_index):
                            mem_idx = base_index + ct_index[x]
                            adc_usage = int(unit_config.get(f'ADC_{x+1}', 0))
                            if adc_usage == 7 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{2101}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                            elif adc_usage == 8 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{2102}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                            elif adc_usage == 9 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{2103}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                            elif adc_usage == 10 and mem_idx < len(self.shared_memory):
                                self.send_data['VALUES'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","SENSOR_ID":f"{2104}","VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    # Error 정보 추가
                    mem_idx = base_index + error_index[0]
                    
                    if mem_idx < len(self.shared_memory):
                        if self.shared_memory[mem_idx] == ConstDefine.ERROR_CODE_COMMUNICATION:  # 통신에러 코드는 10으로 전송
                            self.canfd_communication_error_cnt[i] += 1
                            
                            if self.canfd_communication_error_cnt[i] >= 10:   # 10번 연속 통신에러 발생 시 통신에러 코드 전송
                                self.send_data['ERROR'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","CODE":f"10"})
                        else:   # 나머지 에러 코드는 그대로 전송    
                            self.send_data['ERROR'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","CODE":f"{self.shared_memory[mem_idx]}"})
                            self.canfd_communication_error_cnt[i] = 0
                    # State 정보 추가
                    status_idx = base_index + 0x18
                    index_idx = base_index + 0x17
                    
                    if status_idx < len(self.shared_memory) and index_idx < len(self.shared_memory):
                        stage = self.shared_memory[status_idx] >> 16
                        status_code = self.shared_memory[status_idx] & 0x000000FF
                        
                        status_map = {0: "None", 1: "Stop", 2: "Run", 3: "Pause", 4: "Initial", 5: "Error"}
                        status = status_map.get(status_code, "NotDefine")
                        
                        index_number = self.shared_memory[index_idx]
                        self.shared_memory[index_idx] = self.shared_memory[index_idx] + 1
                        self.send_data['STATE'].append({"TANK_ID":f"{unit_config.get('TANK_ID', 0)}","STAGE":f"{stage}","STATUS":status, "INDEX":f"{index_number}"})
                        
                except KeyError as e:
                    self.logging.error(f'Config key error for TANK {i}: {e}')
                except IndexError as e:
                    self.logging.error(f'Index error for TANK {i}: {e}')
                except Exception as e:
                    self.logging.error(f'Error processing TANK {i}: {e}')                 
        except Exception as e:
            self.logging.error(f'Critical error in make_json_data: {e}')
            self.consecutive_errors += 1
    
    def timer_update_task(self):
        """Timer 기반 주기적 상태 업데이트 with 예외 처리"""
        try:
            # Timer 중복 실행 방지
            with self.timer_lock:
                if not self.timer_active:
                    return
            
            # GET_STATUS 명령 전송
            # 2025.12.09 - @K2H CAN FD 멀티 마스터 구현. 이젠 유닛보드가 자신의 ID로 데이터 전송.
            # for x in range(self.max_unit_board):
            #     try:
            #         data = {"UNIT_ID": x, "CMD": "GET_STATUS", "SEND": False}
            #         self.tcp_queue.put(data, block=False)
            #         time.sleep(0.1)     # 시간 조절 해서 모든 유닛보드가 1초안에 GET_STATUS 명령을 보낼 수 있도록 함
            #     except queue.Full:
            #         self.logging.warning(f'TCP queue full, skipping unit {x}')
            #     except Exception as e:
            #         self.logging.error(f'Error sending GET_STATUS for unit {x}: {e}')
            
            # JSON 데이터 생성
            try:
                self.make_json_data()
                time.sleep(0.5)  # shared memory에서 지연시간이 없으면 문제 발생
            except Exception as e:
                self.logging.error(f'Error in make_json_data: {e}')
            
            # 소켓으로 데이터 전송
            try:
                if self.client and hasattr(self.client, '_closed') and not self.client._closed:
                    json_data = bytes(json.dumps(self.send_data), 'UTF-8')
                    try:
                        self.socket_send_queue.put(json_data, block=False)
                        self.consecutive_errors = 0  # 성공 시 에러 카운터 리셋
                    except queue.Full:
                        # 큐가 가득 차면 오래된 데이터 제거 후 재시도
                        try:
                            self.socket_send_queue.get_nowait()
                            self.socket_send_queue.put(json_data, block=False)
                            self.logging.warning('Socket send queue full, dropped oldest message')
                        except Exception as e:
                            self.logging.error(f'Failed to handle full socket queue: {e}')
                            self.consecutive_errors += 1
            except Exception as e:
                self.logging.error(f'Error sending data to socket queue: {e}')
                self.consecutive_errors += 1
            
        except Exception as e:
            self.logging.error(f'Critical error in timer_update_task: {e}')
            self.logging.error(traceback.format_exc())
            self.consecutive_errors += 1
        finally:
            # Timer 재시작 (중복 방지)
            try:
                with self.timer_lock:
                    if self.timer_active:
                        Timer(1, self.timer_update_task).start()
            except Exception as e:
                self.logging.error(f'Error restarting timer: {e}')
                       
    def run(self):
        """송신 스레드 메인 루프 with 예외 처리"""
        try:
            common_config = self.config_file['common']
            ip = common_config.get('HOST', 'localhost')
            port = int(common_config.get('PORT1', 5001))
            SERVER_ADDR = (ip, port)
            timeout_seconds = 5
            
            # Timer 시작
            with self.timer_lock:
                self.timer_active = True
            Timer(1, self.timer_update_task).start()
            self.logging.info('Timer started for status updates')
            
            reconnect_delay = 1  # 초기 재연결 대기 시간
            max_reconnect_delay = 60
            
            while True:
                client = None
                try: 
                    self.logging.info(f'Attempting to connect to transmit server {ip}:{port}')
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.settimeout(timeout_seconds)
                    client.connect(SERVER_ADDR)
                    self.logging.info(f'송신 서버에 연결 되었습니다. {ip}:{port}')
                    
                    self.client = client
                    reconnect_delay = 1  # 연결 성공 시 재연결 대기 시간 리셋
                    self.consecutive_errors = 0
                    
                    while True:
                        ######################################################################################################  
                        # 2023-06-19-@K2H 
                        # 서버에 데이터를 전송하기 전에 필요 데이터 정렬
                        if self.receive_event.is_set():
                            self.receive_event.clear()
                            self.logging.warning('Receive event triggered, reconnecting')
                            raise socket.error('Receive event triggered')
                        
                        # 펌웨어 업데이트 등으로 인한 전송 일시 중지 대기
                        self.transmit_event.wait()
                        
                        try:
                            #send_data = self.socket_send_queue.get_nowait(timeout=5.0)
                            # get_nowait() 대신 timeout을 주어 타이밍 이슈 완화 및 CPU 부하 감소
                            send_data = self.socket_send_queue.get(timeout=0.1)
                        except queue.Empty:
                            continue
                        
                        # 전송 직전에 한 번 더 체크 (wait 이후 상태 변경 가능성 대비)
                        if not self.transmit_event.is_set():
                            continue
                        
                        if client and hasattr(client, '_closed') and not client._closed:
                            try:
                                client.sendall(send_data)
                                self.consecutive_errors = 0
                            except socket.error as send_error:
                                self.logging.error(f"Send error: {send_error}")
                                self.consecutive_errors += 1
                                raise socket.error(send_error)
                            except Exception as e:
                                self.logging.error(f"Unexpected send error: {e}")
                                self.consecutive_errors += 1
                                raise
                        
                        ######################################################################################################
                        # 데이터 크기에 따른 대기 시간 조정
                        if len(send_data) > 1024:
                            time.sleep(0.5)   
                        else:
                            time.sleep(0.1)
                            
                except socket.timeout:
                    self.logging.warning(f"Transmitter connection timeout to {ip}:{port}")
                    self.consecutive_errors += 1
                    
                except socket.error as e:
                    self.logging.error(f"Transmitter socket error: {e}")
                    self.consecutive_errors += 1
                    
                    # 재연결 전 GET_STATUS 명령 전송
                    for x in range(self.max_unit_board):
                        try:
                            data = {"UNIT_ID": x, "CMD": "GET_STATUS", "SEND": False}
                            self.tcp_queue.put(data, block=False)
                            time.sleep(0.1)
                        except Exception as e:
                            self.logging.error(f'Error sending GET_STATUS during reconnect: {e}')
                    
                except RemoteError as e:
                    self.logging.critical(f"Multiprocessing Manager RemoteError: {e}")
                    self.logging.critical("Program requires restart due to broken IPC.")
                    os._exit(1)
                    
                except Exception as e:
                    self.logging.error(f"Unexpected error in transmitter: {e}")
                    self.logging.error(traceback.format_exc())
                    self.consecutive_errors += 1
                    
                finally:
                    # 소켓 정리
                    try:
                        if client and hasattr(client, '_closed') and not client._closed:
                            client.close()
                            self.logging.info('Transmitter socket closed')
                    except Exception as e:
                        self.logging.error(f'Error closing transmitter socket: {e}')
                    
                    self.client = None
                    
                    # 재연결 대기 (exponential backoff)
                    if self.consecutive_errors > self.max_error_threshold:
                        self.logging.warning(f'Too many consecutive errors ({self.consecutive_errors}), increasing backoff')
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                    
                    self.logging.info(f'Reconnecting to transmit server in {reconnect_delay} seconds...')
                    time.sleep(reconnect_delay)
                    
        except Exception as e:
            self.logging.critical(f'Critical error in UnitBoardGetStatus run loop: {e}')
            self.logging.critical(traceback.format_exc())
        finally:
            # Timer 정지
            with self.timer_lock:
                self.timer_active = False
            self.logging.info('UnitBoardGetStatus thread stopped')
                
def extract_json_objects(buffer_str):
    """
    버퍼 문자열에서 완전한 JSON 객체들을 추출합니다.
    여러 JSON이 연속으로 붙어있어도 하나씩 분리합니다.
    
    Returns:
        (list of dict, remaining_str): 파싱된 JSON 객체 리스트와 남은 문자열
    """
    json_objects = []
    remaining = buffer_str
    
    while remaining:
        remaining = remaining.lstrip()  # 앞쪽 공백 제거
        if not remaining:
            break
            
        if not remaining.startswith('{'):
            # JSON 객체가 아닌 경우 - 다음 '{'를 찾음
            next_brace = remaining.find('{')
            if next_brace == -1:
                remaining = ''  # JSON이 없으면 버림
                break
            remaining = remaining[next_brace:]
        
        # 중괄호 균형을 추적하여 첫 번째 완전한 JSON 객체의 끝을 찾음
        brace_count = 0
        in_string = False
        escape_next = False
        end_pos = -1
        
        for i, char in enumerate(remaining):
            if escape_next:
                escape_next = False
                continue
                
            if char == '\\' and in_string:
                escape_next = True
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
                
            if in_string:
                continue
                
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    end_pos = i + 1
                    break
        
        if end_pos == -1:
            # 완전한 JSON을 찾지 못함 - 더 많은 데이터 필요
            break
        
        # 첫 번째 완전한 JSON 객체 추출
        json_str = remaining[:end_pos]
        try:
            json_obj = json.loads(json_str)
            json_objects.append(json_obj)
            remaining = remaining[end_pos:]
        except json.decoder.JSONDecodeError:
            # 파싱 실패 - 다음 '{'부터 다시 시도
            remaining = remaining[1:]
    
    return json_objects, remaining


class TcpServerThread(threading.Thread):
    def __init__(self, tcp_queue, logging, GPIOADDR1, GPIOADDR2, socket_event, 
                 i2c_semaphor, MAXUNITBOARD, shm_name, unit_np_shm, socket_send_queue, status_control_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.logging = logging
        self.tcp_queue = tcp_queue
        self.GPIOADDR1 = GPIOADDR1
        self.GPIOADDR2 = GPIOADDR2
        self.i2c_semaphor = i2c_semaphor
        self.event = threading.Event()
        self.max_unit_board = MAXUNITBOARD
        self.shm_name = shm_name
        self.unit_np_shm = unit_np_shm
        self.socket_send_queue = socket_send_queue
        self.socket_event = socket_event
        self.status_control_queue = status_control_queue
        
        # 에러 카운터
        self.consecutive_errors = 0
        self.max_error_threshold = 10
        self.i2c_error_count = 0
        
        # Shared memory 접근 with 예외 처리
        try:
            self.new_shm = shared_memory.SharedMemory(name=self.shm_name)
            self.shared_memory = np.ndarray(unit_np_shm.shape, dtype=unit_np_shm.dtype, buffer=self.new_shm.buf)
            self.logging.info(f'Shared memory attached: {self.shm_name}')
        except FileNotFoundError:
            self.logging.error(f'Shared memory not found: {self.shm_name}')
            raise
        except Exception as e:
            self.logging.error(f'Failed to attach shared memory: {e}')
            raise
    
    def i2c_write_with_retry(self, address, register, value, max_retries=3):
        """I2C 쓰기 with 재시도 로직"""
        for attempt in range(max_retries):
            try:
                self.i2c_semaphor.acquire(timeout=2.0)
                try:
                    i2cbus = smbus.SMBus(1)
                    i2cbus.write_byte_data(address, register, value)
                    i2cbus.close()
                    self.i2c_error_count = 0  # 성공 시 에러 카운터 리셋
                    return True
                finally:
                    self.i2c_semaphor.release()
            except OSError as e:
                self.i2c_error_count += 1
                self.logging.error(f'I2C write error (attempt {attempt+1}/{max_retries}): {e}')
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
            except Exception as e:
                self.i2c_error_count += 1
                self.logging.error(f'Unexpected I2C error (attempt {attempt+1}/{max_retries}): {e}')
                if attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
        
        self.logging.error(f'I2C write failed after {max_retries} retries')
        return False
    
    def i2c_led_control(self, on=True):
        """I2C LED 제어 with 재시도"""
        if on:
            # LED ON
            self.i2c_write_with_retry(self.GPIOADDR1, 0x12, 0xFF)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x13, 0xFF)

            self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0xFF)
            self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0xFF)
        else:
            # LED OFF (blink)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x12, 0xFF)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x13, 0xFF)

            self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0xFF)
            self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0xFF)
            time.sleep(0.5)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x12, 0x00)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x13, 0x00)

            self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0x00)
            self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0x00)
    
    def run(self):
        """수신 스레드 메인 루프 with 예외 처리"""
        self.logging.info('TcpServerThread 시작')
        
        # Config 파일 읽기 with 예외 처리
        config_file = configparser.ConfigParser()
        try:
            config_result = config_file.read('/home/pi/Projects/cosmo-m/config/config.ini')
            if not config_result:
                self.logging.error('Config file not found, using default values')
                config_file['common'] = {'HOST': 'localhost', 'PORT2': '5002'}
        except Exception as e:
            self.logging.error(f'Failed to read config file: {e}')
            config_file['common'] = {'HOST': 'localhost', 'PORT2': '5002'}
        
        common_config = config_file['common']
        
        # Status 스레드 시작
        receive_event = threading.Event()
        try:
            status_thread = UnitBoardGetStatus(self.logging, self.tcp_queue, self.max_unit_board, 
                                              self.shared_memory, self.socket_send_queue, receive_event, self.status_control_queue)
            status_thread.start()
            self.logging.info('UnitBoardGetStatus thread started')
        except Exception as e:
            self.logging.error(f'Failed to start status thread: {e}')
            return
                    
        ip = common_config.get('HOST', 'localhost')
        port = int(common_config.get('PORT2', 5002))
        SERVER_ADDR = (ip, port)
        timeout_seconds = 20
        old_data = None
        same_data_cnt = 0
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while True:
            client = None
            try:
                self.logging.info(f'Attempting to connect to receive server {ip}:{port}')
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.settimeout(timeout_seconds)
                client.connect(SERVER_ADDR)
                self.logging.info(f'수신 서버에 연결 되었습니다. {ip}:{port}')
                
                self.socket_event.set()
                reconnect_delay = 1  # 연결 성공 시 리셋
                self.consecutive_errors = 0
                
                # I2C LED ON with 재시도
                self.i2c_led_control(on=True)
                
                # 수신 루프
                receive_buffer = ""  # 완전한 JSON을 받기 위한 문자열 버퍼
                while True:
                    try:
                        # 데이터 수신
                        part = client.recv(8192)  # 8KB 버퍼로 확대
                        if not part:  # 연결이 끊어진 경우
                            self.logging.warning("Connection closed by server")
                            raise socket.error("Connection closed by server")
                        
                        try:
                            receive_buffer += part.decode('UTF-8')
                        except UnicodeDecodeError:
                            # 불완전한 UTF-8 - 버퍼에 바이트로 임시 저장 후 재시도
                            self.logging.debug('Incomplete UTF-8 sequence, waiting for more data')
                            continue
                        
                        self.logging.debug(f'Received {len(part)} bytes, buffer size: {len(receive_buffer)} bytes')
                        
                        # 버퍼에서 완전한 JSON 객체들을 추출
                        json_objects, receive_buffer = extract_json_objects(receive_buffer)
                        
                        if not json_objects:
                            # 완전한 JSON이 없으면 더 기다림
                            continue
                        
                        self.consecutive_errors = 0  # 성공 시 에러 카운터 리셋
                        
                        # 추출된 모든 JSON 객체 처리
                        for data in json_objects:
                            try:
                                self.logging.debug(f'Processing JSON object: CMD={data.get("CMD", "unknown")}')
                                
                                # TCP 큐에 데이터 전달
                                try:
                                    self.tcp_queue.put(data, block=False)
                                except queue.Full:
                                    self.logging.warning('TCP queue full, data may be dropped')
                                
                                # ACK 응답 전송
                                try:
                                    ack_msg = bytes(json.dumps({'CMD':'ACK',
                                                                'IDX': data.get('IDX', 'unknown'),
                                                                'NOTE': 'OK'
                                                                }), 'UTF-8')
                                    self.socket_send_queue.put(ack_msg, block=False)
                                except queue.Full:
                                    self.logging.warning('Socket send queue full, ACK may be dropped')
                                except Exception as e:
                                    self.logging.error(f'Error sending ACK: {e}')
                                
                                # 중복 데이터 체크
                                if data.get('IDX') == old_data:
                                    same_data_cnt += 1
                                    if same_data_cnt > 2:
                                        self.logging.warning(f'Duplicate data detected (IDX: {old_data}), clearing queue')
                                        same_data_cnt = 0
                                        
                                        # 큐 비우기
                                        try:
                                            while not self.socket_send_queue.empty():
                                                self.socket_send_queue.get_nowait()
                                        except Exception as e:
                                            self.logging.error(f'Error clearing queue: {e}')
                                        
                                        # 재전송 ACK
                                        try:
                                            ack_msg = bytes(json.dumps({'CMD':'ACK',
                                                                        'IDX': data.get('IDX', 'unknown'),
                                                                        'NOTE': 'OK'
                                                                        }), 'UTF-8')
                                            self.socket_send_queue.put(ack_msg, block=False)
                                        except Exception as e:
                                            self.logging.error(f'Error sending duplicate ACK: {e}')
                                        
                                        receive_event.set()
                                else:
                                    same_data_cnt = 0
                                old_data = data.get('IDX')
                                
                            except KeyError as e:
                                self.logging.error(f'Missing key in JSON data: {e}')
                                self.consecutive_errors += 1
                                
                            except Exception as e:
                                self.logging.error(f'Unexpected error processing data: {e}')
                                self.logging.error(traceback.format_exc())
                                self.consecutive_errors += 1
                        
                        # 여러 메시지 처리 후 짧은 대기
                        if len(json_objects) > 1:
                            self.logging.debug(f'Processed {len(json_objects)} JSON objects in one batch')
                        time.sleep(0.01)
                        
                    except socket.timeout:
                        # 타임아웃 발생 시 버퍼에 데이터가 있으면 처리 시도
                        if len(receive_buffer) > 0:
                            json_objects, receive_buffer = extract_json_objects(receive_buffer)
                            
                            if json_objects:
                                self.consecutive_errors = 0
                                for data in json_objects:
                                    try:
                                        self.tcp_queue.put(data, block=False)
                                        ack_msg = bytes(json.dumps({'CMD':'ACK',
                                                                    'IDX': data.get('IDX', 'unknown'),
                                                                    'NOTE': 'OK'
                                                                    }), 'UTF-8')
                                        self.socket_send_queue.put(ack_msg, block=False)
                                        old_data = data.get('IDX')
                                    except Exception as e:
                                        self.logging.error(f'Error processing data after timeout: {e}')
                            
                            # 남은 불완전한 데이터가 너무 크면 경고
                            if len(receive_buffer) > 65536:  # 64KB 이상
                                self.logging.warning(f"Buffer too large ({len(receive_buffer)} bytes), clearing")
                                receive_buffer = ""
                                self.consecutive_errors += 1
                                
                                if self.consecutive_errors >= self.max_error_threshold:
                                    self.logging.error('Too many errors, reconnecting')
                                    raise socket.error('Too many errors')
                        else:
                            self.logging.debug("Socket Data receive timeout, retrying...")
                            continue
                        
            except socket.timeout:
                self.logging.warning(f"Receiver connection timeout to {ip}:{port}")
                self.consecutive_errors += 1
                
            except socket.error as e:
                self.logging.error(f"Receiver socket error: {e}")
                self.consecutive_errors += 1
                
                # I2C LED OFF (blink) with 재시도
                self.i2c_led_control(on=False)
                
            except Exception as e:
                self.logging.error(f"Unexpected error in receiver: {e}")
                self.logging.error(traceback.format_exc())
                self.consecutive_errors += 1
                
            finally:
                # 소켓 정리
                try:
                    if client and hasattr(client, '_closed') and not client._closed:
                        client.close()
                        self.logging.info('Receiver socket closed')
                except Exception as e:
                    self.logging.error(f'Error closing receiver socket: {e}')
                
                # 재연결 대기 (exponential backoff)
                if self.consecutive_errors > self.max_error_threshold:
                    self.logging.warning(f'Too many consecutive errors ({self.consecutive_errors}), increasing backoff')
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                
                self.logging.info(f'Reconnecting to receive server in {reconnect_delay} seconds...')
                time.sleep(reconnect_delay)
