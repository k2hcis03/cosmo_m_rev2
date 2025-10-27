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
import traceback
import configparser
from signal import signal, SIGPIPE, SIG_DFL
# signal(SIGPIPE,SIG_DFL)

class UnitBoardGetStatus(threading.Thread):
    def __init__(self, logging, tcp_queue, max_unit_board, shared_memory, socket_send_queue, receive_event):
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
        
        # Config 파일 읽기 with 예외 처리
        self.config_file = configparser.ConfigParser()
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
                # "CODE":{"CODE":1000,"MSG":"OK"}
            }
            temp_index = [0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12]           #온도 값이 저장되는 shared_memory 위치 
            humi_index = [0x13, 0x15]                       #습도 값이 저장되는 shared_memory 위치
            co2_index = [0x26, 0x26]                        #Co2 값이 저장되는 shared_memory 위치
            ph_index = [0x28, 0x28]                         #PH 값이 저장되는 shared_memory 위치
            brix_index = [0x25, 0x25]                       #Brix 값이 저장되는 shared_memory 위치
            flow_index = [0x27, 0x27]                       #Flow 값이 저장되는 shared_memory 위치
            loadcell_index = [0x24, 0x24]                   #Load Cell 값이 저장되는 shared_memory 위치
            gpo_index = [0x1A, 0x1A]                        #gpo 값이 저장되는 shared_memory 위치
            gpi_index = [0x1c, 0x1c]                        #gpi 값이 저장되는 shared_memory 위치
            inverter_index = [0x1E, 0x1E]                   #inverter 값이 저장되는 shared_memory 위치
            motor_index = [0x23, 0x23]                      #motor 값이 저장되는 shared_memory 위치
            # vavle_index = [1, 0]
            self.order += 1
                                
            for i in range(int(common_config['FERMEN_TANK'])):
                try:
                    unit_config = self.config_file[f'unit_board{i}']
                    
                    # Shared memory 인덱스 범위 검증
                    base_index = i * size
                    if base_index >= len(self.shared_memory):
                        self.logging.warning(f'Shared memory index out of range for FERMEN_TANK {i}')
                        continue
                    
                    for x in range(int(unit_config.get('TEMP_NUM', 0))):
                        mem_idx = base_index + temp_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{100+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('HUMI_NUM', 0))):
                        mem_idx = base_index + humi_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{200+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('CO2_NUM', 0))):
                        mem_idx = base_index + co2_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{300+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('LOAD_CELL', 0))):
                        mem_idx = base_index + loadcell_index[x]        #11
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{400+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('VALVE_NUM', 0))):
                        mem_idx = base_index + gpo_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{500+x}',"VALUE":                   # 상단, 하단 솔밸브
                                f"{(self.shared_memory[mem_idx] >> x) & 0x00000001}"})
                    
                    for x in range(int(unit_config.get('MOTOR_NUM', 0))):
                        mem_idx = base_index + motor_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{100+i}',"SENSOR_ID":f'{600+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
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
                        self.send_data['STATE'].append({"TANK_ID":f'{100+i}',"STAGE":f'{stage}',"STATUS":status, "INDEX":f'{index_number}'})
                        
                except KeyError as e:
                    self.logging.error(f'Config key error for FERMEN_TANK {i}: {e}')
                except IndexError as e:
                    self.logging.error(f'Index error for FERMEN_TANK {i}: {e}')
                except Exception as e:
                    self.logging.error(f'Error processing FERMEN_TANK {i}: {e}') 
            
            cnt = int(common_config['FERMEN_TANK'])
            
            for i in range(int(common_config['BLEND_TANK'])):
                try:
                    unit_config = self.config_file[f'unit_board{cnt}']
                    base_index = (i + cnt) * size
                    
                    if base_index >= len(self.shared_memory):
                        self.logging.warning(f'Shared memory index out of range for BLEND_TANK {i}')
                        continue
                    
                    for x in range(int(unit_config.get('TEMP_NUM', 0))):
                        mem_idx = base_index + temp_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{100+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('HUMI_NUM', 0))):
                        mem_idx = base_index + humi_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{200+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('CO2_NUM', 0))):
                        mem_idx = base_index + co2_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{300+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('LOAD_CELL', 0))):
                        mem_idx = base_index + 11
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{400+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.01:0.2F}"})
                    
                    for x in range(int(unit_config.get('VALVE_NUM', 0))):
                        mem_idx = base_index + 6
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{500+x}',"VALUE":
                                f"{(self.shared_memory[mem_idx] & (0x000000FF << x*8)) >> x*8}"})
                    
                    for x in range(int(unit_config.get('MOTOR_NUM', 0))):
                        mem_idx = base_index + 10
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{200+i}',"SENSOR_ID":f'{600+x}',"VALUE":f"{self.shared_memory[mem_idx]}"})
                    
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
                        self.send_data['STATE'].append({"TANK_ID":f'{200+i}',"STAGE":f'{stage}',"STATUS":status, "INDEX":f'{index_number}'})
                        
                except KeyError as e:
                    self.logging.error(f'Config key error for BLEND_TANK {i}: {e}')
                except IndexError as e:
                    self.logging.error(f'Index error for BLEND_TANK {i}: {e}')
                except Exception as e:
                    self.logging.error(f'Error processing BLEND_TANK {i}: {e}') 
                        
            cnt = int(common_config['FERMEN_TANK']) + int(common_config['BLEND_TANK'])  
            for i in range(int(common_config['PROD_TANK'])):
                try:
                    unit_config = self.config_file[f'unit_board{cnt}']
                    base_index = (i + cnt) * size
                    
                    if base_index >= len(self.shared_memory):
                        self.logging.warning(f'Shared memory index out of range for PROD_TANK {i}')
                        continue
                    
                    for x in range(int(unit_config.get('TEMP_NUM', 0))):
                        mem_idx = base_index + temp_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{100+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('HUMI_NUM', 0))):
                        mem_idx = base_index + humi_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{200+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('CO2_NUM', 0))):
                        mem_idx = base_index + co2_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{300+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('LOAD_CELL', 0))):
                        mem_idx = base_index + 11
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{400+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('VALVE_NUM', 0))):
                        mem_idx = base_index + 6
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{500+x}',"VALUE":
                                f"{(self.shared_memory[mem_idx] & (0x000000FF << x*8)) >> x*8}"})
                    
                    for x in range(int(unit_config.get('MOTOR_NUM', 0))):
                        mem_idx = base_index + 10
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{300+i}',"SENSOR_ID":f'{600+x}',"VALUE":f"{self.shared_memory[mem_idx]}"})
                    
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
                        self.send_data['STATE'].append({"TANK_ID":f'{300+i}',"STAGE":f'{stage}',"STATUS":status, "INDEX":f'{index_number}'})
                        
                except KeyError as e:
                    self.logging.error(f'Config key error for PROD_TANK {i}: {e}')
                except IndexError as e:
                    self.logging.error(f'Index error for PROD_TANK {i}: {e}')
                except Exception as e:
                    self.logging.error(f'Error processing PROD_TANK {i}: {e}')
            
            cnt = int(common_config['FERMEN_TANK']) + int(common_config['BLEND_TANK']) + int(common_config['PROD_TANK'])   
            for i in range(int(common_config['CHILER_TANK'])):
                try:
                    unit_config = self.config_file[f'unit_board{cnt}']
                    base_index = (i + cnt) * size
                    
                    if base_index >= len(self.shared_memory):
                        self.logging.warning(f'Shared memory index out of range for CHILER_TANK {i}')
                        continue
                    
                    for x in range(int(unit_config.get('TEMP_NUM', 0))):
                        mem_idx = base_index + temp_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{100+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('HUMI_NUM', 0))):
                        mem_idx = base_index + humi_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{200+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('CO2_NUM', 0))):
                        mem_idx = base_index + co2_index[x]
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{300+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('LOAD_CELL', 0))):
                        mem_idx = base_index + 11
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{400+x}',"VALUE":f"{self.shared_memory[mem_idx]*0.001:0.2F}"})
                    
                    for x in range(int(unit_config.get('VALVE_NUM', 0))):
                        mem_idx = base_index + 6
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{500+x}',"VALUE":
                                f"{(self.shared_memory[mem_idx] & (0x000000FF << x*8)) >> x*8}"})
                    
                    for x in range(int(unit_config.get('MOTOR_NUM', 0))):
                        mem_idx = base_index + 10
                        if mem_idx < len(self.shared_memory):
                            self.send_data['VALUES'].append({"TANK_ID":f'{400+i}',"SENSOR_ID":f'{600+x}',"VALUE":f"{self.shared_memory[mem_idx]}"})
                    
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
                        self.send_data['STATE'].append({"TANK_ID":f'{400+i}',"STAGE":f'{stage}',"STATUS":status, "INDEX":f'{index_number}'})
                        
                except KeyError as e:
                    self.logging.error(f'Config key error for CHILER_TANK {i}: {e}')
                except IndexError as e:
                    self.logging.error(f'Index error for CHILER_TANK {i}: {e}')
                except Exception as e:
                    self.logging.error(f'Error processing CHILER_TANK {i}: {e}')
                    
        except Exception as e:
            self.logging.error(f'Critical error in make_json_data: {e}')
            self.consecutive_errors += 1
    
    def timer_upadate_task(self):
        """Timer 기반 주기적 상태 업데이트 with 예외 처리"""
        try:
            # Timer 중복 실행 방지
            with self.timer_lock:
                if not self.timer_active:
                    return
            
            # GET_STATUS 명령 전송
            for x in range(self.max_unit_board):
                try:
                    data = {"UNIT_ID": x, "CMD": "GET_STATUS", "SEND": False}
                    self.tcp_queue.put(data, block=False)
                    time.sleep(0.1)     # 시간 조절 해서 모든 유닛보드가 1초안에 GET_STATUS 명령을 보낼 수 있도록 함
                except queue.Full:
                    self.logging.warning(f'TCP queue full, skipping unit {x}')
                except Exception as e:
                    self.logging.error(f'Error sending GET_STATUS for unit {x}: {e}')
            
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
                        Timer(1, self.timer_upadate_task).start()
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
            Timer(1, self.timer_upadate_task).start()
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
                        
                        try:
                            #send_data = self.socket_send_queue.get_nowait(timeout=5.0)
                            send_data = self.socket_send_queue.get_nowait()
                        except queue.Empty:
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
                
class TcpClientThread(threading.Thread):
    def __init__(self, tcp_queue, logging, GPIOADDR1, GPIOADDR2, socket_event, 
                 i2c_semaphor, MAXUNITBOARD, shm_name, unit_np_shm, socket_send_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.logging = logging
        self.tcp_queue = tcp_queue
        self.GPIOADDR1 = GPIOADDR1
        self.GPIOADDR2 =GPIOADDR2
        self.i2c_semaphor = i2c_semaphor
        self.event = threading.Event()
        self.max_unit_board = MAXUNITBOARD
        self.shm_name = shm_name
        self.unit_np_shm = unit_np_shm
        self.socket_send_queue = socket_send_queue
        self.socket_event = socket_event
        
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

            # self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0xFF)
            # self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0xFF)
        else:
            # LED OFF (blink)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x12, 0xFF)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x13, 0xFF)

            # self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0xFF)
            # self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0xFF)
            time.sleep(0.5)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x12, 0x00)
            self.i2c_write_with_retry(self.GPIOADDR1, 0x13, 0x00)

            # self.i2c_write_with_retry(self.GPIOADDR2, 0x12, 0x00)
            # self.i2c_write_with_retry(self.GPIOADDR2, 0x13, 0x00)
    
    def run(self):
        """수신 스레드 메인 루프 with 예외 처리"""
        self.logging.info('TcpClientThread 시작')
        
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
                                              self.shared_memory, self.socket_send_queue, receive_event)
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
                while True:
                    data = bytearray()
                    try:
                        # 데이터 수신
                        while True:
                            part = client.recv(4096)  # 4KB 버퍼로 최적화
                            if not part:  # 연결이 끊어진 경우
                                self.logging.warning("Connection closed by server")
                                raise socket.error("Connection closed by server")
                            data += part
                            if len(part) < 4096:  # 버퍼 크기에 맞춰 수정
                                # either 0 or end of data
                                break
                        
                        self.logging.debug(f'Received {len(data)} bytes')
                        self.consecutive_errors = 0  # 성공 시 에러 카운터 리셋
                        
                    except socket.timeout:
                        self.logging.warning("Data receive timeout, retrying...")
                        continue
                    
                    # JSON 파싱 및 처리
                    try:
                        decoded_data = bytes(data).decode('UTF-8')
                        data = json.loads(decoded_data)
                        
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
                        
                        time.sleep(0.05)
                        
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
                        
                    except UnicodeDecodeError as e:
                        self.logging.error(f'UTF-8 decode error: {e}')
                        self.consecutive_errors += 1
                        try:
                            index = '999'  # JSON 에러 발생 시, 'IDX'값이 쓰레기가 있기때문에 '999'을 강제 셋팅
                            error_msg = bytes(json.dumps({'CMD':'ACK',
                                                          'IDX': index,
                                                          'NOTE': 'DecodeError'
                                                          }), 'UTF-8')
                            self.socket_send_queue.put(error_msg, block=False)
                        except Exception:
                            pass
                        
                    except json.decoder.JSONDecodeError as e:
                        self.logging.error(f'JSON decode error: {e}')
                        self.logging.error(f'Data: {bytes(data)[:200]}')  # 처음 200바이트만 로깅
                        self.consecutive_errors += 1
                        
                        try:
                            index = '999'  # JSON 에러 발생 시, 'IDX'값이 쓰레기가 있기때문에 '999'을 강제 셋팅
                            error_msg = bytes(json.dumps({'CMD':'ACK',
                                                          'IDX': index,
                                                          'NOTE': 'Resend'
                                                          }), 'UTF-8')
                            self.socket_send_queue.put(error_msg, block=False)
                        except Exception as e2:
                            self.logging.error(f'Error sending error ACK: {e2}')
                        
                        # JSON 에러가 너무 많으면 재연결
                        if self.consecutive_errors >= self.max_error_threshold:
                            self.logging.error('Too many JSON errors, reconnecting')
                            raise socket.error('Too many JSON errors')
                        
                    except KeyError as e:
                        self.logging.error(f'Missing key in JSON data: {e}')
                        self.consecutive_errors += 1
                        
                    except Exception as e:
                        self.logging.error(f'Unexpected error processing data: {e}')
                        self.logging.error(traceback.format_exc())
                        self.consecutive_errors += 1
                        
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
