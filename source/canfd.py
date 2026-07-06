#! /usr/bin/python3
import os
import can
import time
import queue

from concurrent.futures import ThreadPoolExecutor
import threading

class CanFDReceive(threading.Thread):
    def __init__(self, logging, main_func) -> None:
        threading.Thread.__init__(self) 
        self.main_func = main_func
        self.logging = logging
        self.daemon = True
        self.receive_queue = []
        self.error_counter = 0
        self.max_error_threshold = 30  # 연속 30회 에러 시 재초기화
        self.consecutive_errors = 0
        self.total_errors = 0
        self.logging.info('CanFDReceive initialized')
        
    def reinitialize_can_bus(self):
        """CAN 버스 재초기화"""
        with self.main_func.can_lock:
            # 다른 스레드가 방금 재초기화를 마쳤다면 중복으로 다시 수행하지 않음
            if time.time() - self.main_func.can_last_reinit_time < 2.0:
                self.logging.info('CAN bus was just reinitialized by another thread, skipping duplicate reinit')
                self.consecutive_errors = 0
                return True
            try:
                self.logging.warning('Attempting to reinitialize CAN bus...')

                # 기존 CAN 버스 종료
                try:
                    self.main_func.can0.shutdown()
                except Exception as e:
                    self.logging.error(f'Error during CAN shutdown: {e}')
            
                time.sleep(0.5)
            
                # CAN 인터페이스 재설정
                os.system("sudo ifconfig can0 down")
                time.sleep(0.2)
                os.system("sudo ip link set can0 up type can bitrate 1000000 dbitrate 4000000 restart-ms 1000 berr-reporting on fd on")
                os.system("sudo ifconfig can0 txqueuelen 65536")
                time.sleep(0.5)
            
                # CAN 버스 재생성
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
            
                new_bus = can.interface.Bus(
                    rx_fifo_size=8192,
                    channel='can0',
                    interface='socketcan',
                    bitrate_switch=False,
                    bitrate=1000000,
                    data_bitrate=4000000,
                    fd=True,
                    can_filters=filters
                )

                # main_func의 can0을 갱신 (송/수신 스레드가 항상 이를 통해 참조)
                self.main_func.can0 = new_bus
                self.main_func.can_last_reinit_time = time.time()

                # 에러 카운터 리셋
                self.consecutive_errors = 0
                self.logging.info('CAN bus reinitialized successfully')
                return True
            
            except Exception as e:
                self.logging.error(f'Failed to reinitialize CAN bus: {e}')
                return False

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
    
    def run(self): 
        while True:
            try:
                # 타임아웃을 설정하여 recv 호출
                message = self.main_func.can0.recv(timeout=1.0)
                
                if message is None:
                    # 타임아웃 발생 (정상적인 경우일 수 있음)
                    continue
                
                if message.is_error_frame:
                    self.consecutive_errors += 1
                    self.total_errors += 1
                    # self.logging.warning(
                    #     f'CAN bus error frame (0x{message.arbitration_id:X}), '
                    #     f'consecutive: {self.consecutive_errors}'
                    # )
                    if self.consecutive_errors >= self.max_error_threshold:
                        self.logging.critical(
                            f'Too many CAN error frames ({self.consecutive_errors}), reinitializing bus'
                        )
                        if self.reinitialize_can_bus():
                            time.sleep(1.0)
                        else:
                            time.sleep(5.0)
                    continue

                # 정상 수신 시 에러 카운터 리셋
                self.consecutive_errors = 0

                # arbitration_id 범위 확인
                if message.arbitration_id >= 0x100 and message.arbitration_id <= 0x11F:
                    queue_index = message.arbitration_id - 0x100
                    
                    # receive_queue 인덱스 범위 확인
                    if queue_index < len(self.receive_queue):
                        if len(message.data) < 2:
                            self.logging.warning(f'Too short frame for ID 0x{message.arbitration_id:X} (len={len(message.data)})')
                            continue
                        try:
                            # 큐가 가득 찬 경우 처리 calc_crc = self.crc16(message.data[:-2])
                            crc = self.crc16(message.data[:-2])
                            if crc != message.data[-1] | (message.data[-2] << 8):
                                self.logging.warning(f'CRC error for ID 0x{message.arbitration_id:X}')
                                continue
                            self.receive_queue[queue_index].put(message, block=False)
                        except queue.Full:
                            # 큐가 가득 차면 오래된 메시지 제거 후 새 메시지 추가
                            try:
                                self.receive_queue[queue_index].get_nowait()
                                self.receive_queue[queue_index].put(message, block=False)
                                self.logging.warning(f'RX queue full for ID 0x{message.arbitration_id:X}, dropped oldest message')
                            except Exception as e:
                                self.logging.error(f'Failed to handle full queue: {e}')
                    else:
                        self.logging.warning(f'Queue index {queue_index} out of range (queue size: {len(self.receive_queue)})')
                else:
                    self.logging.warning(f'Wrong arbitration_id 0x{message.arbitration_id:X}')
            
            except can.CanError as e:
                # CAN 프로토콜 에러
                self.consecutive_errors += 1
                self.total_errors += 1
                # self.logging.error(f'CAN error in receive: {e} (consecutive errors: {self.consecutive_errors})')
                
                if self.consecutive_errors >= self.max_error_threshold:
                    self.logging.critical(f'Too many consecutive CAN errors ({self.consecutive_errors}), reinitializing bus')
                    if self.reinitialize_can_bus():
                        time.sleep(1.0)
                    else:
                        time.sleep(5.0)
                else:
                    time.sleep(0.1)
            
            except OSError as e:
                # 네트워크/소켓 에러 (TX overflow 등)
                self.consecutive_errors += 1
                self.total_errors += 1
                self.logging.error(f'OSError in receive: {e} (consecutive errors: {self.consecutive_errors})')
                
                if self.consecutive_errors >= self.max_error_threshold:
                    self.logging.critical(f'Too many consecutive OS errors ({self.consecutive_errors}), reinitializing bus')
                    if self.reinitialize_can_bus():
                        time.sleep(1.0)
                    else:
                        time.sleep(5.0)
                else:
                    time.sleep(0.1)
            
            except Exception as e:
                # 기타 예외
                self.consecutive_errors += 1
                self.total_errors += 1
                self.logging.error(f'Unexpected error in receive: {type(e).__name__}: {e} (consecutive errors: {self.consecutive_errors})')
                
                if self.consecutive_errors >= self.max_error_threshold:
                    self.logging.critical(f'Too many consecutive errors ({self.consecutive_errors}), reinitializing bus')
                    if self.reinitialize_can_bus():
                        time.sleep(1.0)
                    else:
                        time.sleep(5.0)
                else:
                    time.sleep(0.5)

class CanFDTransmitte(threading.Thread):
    def __init__(self, logging, main_func) -> None:
        threading.Thread.__init__(self) 
        self.main_func = main_func
        self.logging = logging
        self.logging.info('CanFDTransmitte initialized')
        self.daemon = True
        self.queue = None
        self.consecutive_errors = 0
        self.max_error_threshold = 10  # 연속 10회 에러 시 재초기화
        self.total_errors = 0
        self.max_retries = 3  # 메시지당 최대 재시도 횟수
        
    def reinitialize_can_bus(self):
        """CAN 버스 재초기화"""
        with self.main_func.can_lock:
            # 다른 스레드가 방금 재초기화를 마쳤다면 중복으로 다시 수행하지 않음
            if time.time() - self.main_func.can_last_reinit_time < 2.0:
                self.logging.info('CAN bus was just reinitialized by another thread, skipping duplicate reinit (Transmit)')
                self.consecutive_errors = 0
                return True
            try:
                self.logging.warning('Attempting to reinitialize CAN bus (Transmit)...')

                # 기존 CAN 버스 종료
                try:
                    self.main_func.can0.shutdown()
                except Exception as e:
                    self.logging.error(f'Error during CAN shutdown: {e}')
            
                time.sleep(0.5)
            
                # CAN 인터페이스 재설정
                os.system("sudo ifconfig can0 down")
                time.sleep(0.2)
                os.system("sudo ip link set can0 up type can bitrate 1000000 dbitrate 4000000 restart-ms 1000 berr-reporting on fd on")
                os.system("sudo ifconfig can0 txqueuelen 65536")
                time.sleep(0.5)
            
                # CAN 버스 재생성
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
            
                new_bus = can.interface.Bus(
                    rx_fifo_size=8192,
                    channel='can0',
                    interface='socketcan',
                    bitrate_switch=False,
                    bitrate=1000000,
                    data_bitrate=4000000,
                    fd=True,
                    can_filters=filters
                )

                # main_func의 can0을 갱신 (송/수신 스레드가 항상 이를 통해 참조)
                self.main_func.can0 = new_bus
                self.main_func.can_last_reinit_time = time.time()

                # 에러 카운터 리셋
                self.consecutive_errors = 0
                self.logging.info('CAN bus reinitialized successfully (Transmit)')
                return True
            
            except Exception as e:
                self.logging.error(f'Failed to reinitialize CAN bus (Transmit): {e}')
                return False

    def run(self):
        while True:
            message = None
            try:
                # 타임아웃을 설정하여 큐에서 메시지 가져오기
                message = self.queue.get(timeout=1.0)
                
                # 메시지 전송 재시도 로직
                retry_count = 0
                sent_successfully = False
                
                while retry_count <= self.max_retries and not sent_successfully:
                    try:
                        self.main_func.can0.send(message)
                        sent_successfully = True
                        # 성공 시 에러 카운터 리셋
                        self.consecutive_errors = 0
                        
                    except can.CanError as e:
                        retry_count += 1
                        self.logging.error(f'CAN error during send (attempt {retry_count}/{self.max_retries}): {e}')
                        
                        if retry_count <= self.max_retries:
                            time.sleep(0.05 * retry_count)  # 점진적 백오프
                        else:
                            self.consecutive_errors += 1
                            self.total_errors += 1
                            self.logging.error(f'Failed to send message after {self.max_retries} retries')
                    
                    except OSError as e:
                        # TX overflow 또는 네트워크 에러
                        retry_count += 1
                        
                        if 'No buffer space available' in str(e) or 'errno 105' in str(e):
                            self.logging.error(f'TX buffer overflow (attempt {retry_count}/{self.max_retries})')
                        else:
                            self.logging.error(f'OSError during send (attempt {retry_count}/{self.max_retries}): {e}')
                        
                        if retry_count <= self.max_retries:
                            time.sleep(0.1 * retry_count)  # 버퍼 비워질 시간 대기
                        else:
                            self.consecutive_errors += 1
                            self.total_errors += 1
                            self.logging.error(f'Failed to send message after {self.max_retries} retries: {e}')
                
                # 연속 에러가 임계값을 초과하면 CAN 버스 재초기화
                if self.consecutive_errors >= self.max_error_threshold:
                    self.logging.critical(f'Too many consecutive send errors ({self.consecutive_errors}), reinitializing bus')
                    if self.reinitialize_can_bus():
                        time.sleep(1.0)
                        if not sent_successfully:
                            # 버스가 방금 복구됐으므로 메시지 유실을 막기 위해 한 번 더 시도
                            try:
                                self.main_func.can0.send(message)
                                sent_successfully = True
                                self.consecutive_errors = 0
                            except Exception as e:
                                self.logging.error(f'Retry after bus reinitialization also failed (arbitration_id=0x{message.arbitration_id:X}): {e}')
                    else:
                        time.sleep(5.0)

                if not sent_successfully:
                    self.logging.error(f'Message permanently dropped (arbitration_id=0x{message.arbitration_id:X}, data={bytes(message.data).hex()})')

            except queue.Empty:
                # 큐가 비어있음 (정상)
                continue
            
            except Exception as e:
                # 기타 예외
                self.consecutive_errors += 1
                self.total_errors += 1
                self.logging.error(f'Unexpected error in transmit: {type(e).__name__}: {e} (consecutive errors: {self.consecutive_errors})')

                if self.consecutive_errors >= self.max_error_threshold:
                    self.logging.critical(f'Too many consecutive errors ({self.consecutive_errors}), reinitializing bus')
                    if self.reinitialize_can_bus():
                        time.sleep(1.0)
                    else:
                        time.sleep(5.0)
                else:
                    time.sleep(0.5)

                # 다른 스레드의 재초기화 도중이었을 수 있으므로(예: fd=-1 ValueError), 큐에서 이미 꺼낸
                # 메시지를 재시도 없이 버리지 않도록 마지막으로 한 번 더 전송을 시도
                if message is not None:
                    try:
                        self.main_func.can0.send(message)
                        self.consecutive_errors = 0
                    except Exception as e2:
                        self.logging.error(f'Message permanently dropped (arbitration_id=0x{message.arbitration_id:X}, data={bytes(message.data).hex()}): {e2}')
