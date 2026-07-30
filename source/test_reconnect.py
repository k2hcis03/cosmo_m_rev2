#!/usr/bin/env python3
"""client.py 재연결 로직 회귀 테스트.

라즈베리파이 하드웨어(smbus, SIGPIPE) 없이도 실행할 수 있도록 의존성을 주입한 뒤
client 모듈을 import 한다. smbus는 항상 가짜 객체로 대체되므로, 라즈베리파이에서
실행해도 실제 I2C 하드웨어를 건드리지 않는다.

핵심 검증 항목:
  1. 서버가 FIN 없이 조용히 사라졌을 때(half-open) 수신 소켓이 스스로 재연결하는가
     → 무수신 감지가 없으면 recv 타임아웃마다 continue 하며 영구히 좀비 상태로 남는다.
  2. 재연결 백오프가 '짧게 끊긴 연결'에서는 초기화되지 않는가
  3. 소켓에 TCP keepalive가 실제로 설정되는가
  4. 무수신 판정 임계값이 서버 PING 주기보다 충분히 큰가 (오탐 방지)

실행:
    python3 test_reconnect.py

주의: 127.0.0.1의 7000/7001 포트에 가짜 서버를 띄우므로 해당 포트가 비어 있어야 한다.
"""
import configparser
import logging as pylogging
import os
import queue
import signal
import socket
import sys
import threading
import time
import types
import unittest
from multiprocessing import shared_memory
from unittest import mock

import numpy as np

# ---------------------------------------------------------------------------
# client.py import를 위한 환경 준비 (라즈베리파이 전용 의존성 대체)
# ---------------------------------------------------------------------------
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SOURCE_DIR)

if 'smbus' not in sys.modules:                      # I2C 하드웨어 대체
    fake_smbus = types.ModuleType('smbus')

    class _FakeSMBus:
        def __init__(self, bus):
            self.bus = bus

        def write_byte_data(self, address, register, value):
            pass

        def close(self):
            pass

    fake_smbus.SMBus = _FakeSMBus
    sys.modules['smbus'] = fake_smbus

if not hasattr(signal, 'SIGPIPE'):                  # Windows에는 SIGPIPE가 없다
    signal.SIGPIPE = 13

import client  # noqa: E402  (의존성 주입 이후에 import 해야 한다)

RECEIVE_PORT = 7001     # config를 못 읽을 때 client.py가 쓰는 기본 수신 포트
TRANSMIT_PORT = 7000    # 기본 송신 포트


class SilentServer(threading.Thread):
    """접속을 받아 PING 한 번만 보내고, 그 뒤로는 close 하지 않고 침묵하는 서버.

    FIN을 보내지 않으므로 클라이언트의 recv는 에러가 아니라 타임아웃만 반복한다.
    즉 '경로가 조용히 죽은' 상황(NAT 만료, 서버 half-open)을 그대로 재현한다.
    """

    def __init__(self, port, send_ping=True):
        super().__init__(daemon=True)
        self.port = port
        self.send_ping = send_ping
        self.accept_times = []          # 접속이 수립된 시각(monotonic)
        self.accepted = threading.Event()
        self._sockets = []              # 침묵 유지를 위해 참조를 붙잡아 둔다
        self._stop = threading.Event()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', port))
        self.listener.listen(8)
        self.listener.settimeout(0.5)

    def run(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.accept_times.append(time.monotonic())
            self._sockets.append(conn)
            self.accepted.set()
            if self.send_ping:
                try:
                    conn.sendall(b'{"CMD":"PING","IDX":"1234","NOTE":"OK"}')
                except OSError:
                    pass
            # 이후 아무것도 보내지 않고, 닫지도 않는다.

    def stop(self):
        self._stop.set()
        for sock in self._sockets:
            try:
                sock.close()
            except OSError:
                pass
        try:
            self.listener.close()
        except OSError:
            pass


class DrainServer(SilentServer):
    """송신 포트용. 접속을 받아 들어오는 데이터를 계속 버린다."""

    def __init__(self, port):
        super().__init__(port, send_ping=False)

    def run(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.accept_times.append(time.monotonic())
            self._sockets.append(conn)
            self.accepted.set()
            threading.Thread(target=self._drain, args=(conn,), daemon=True).start()

    def _drain(self, conn):
        conn.settimeout(0.5)
        while not self._stop.is_set():
            try:
                if not conn.recv(65536):
                    return
            except socket.timeout:
                continue
            except OSError:
                return


def make_logger():
    logger = pylogging.getLogger('test_reconnect')
    logger.setLevel(pylogging.DEBUG)
    if not logger.handlers:
        handler = pylogging.StreamHandler(sys.stdout)
        handler.setFormatter(pylogging.Formatter('    [%(levelname)s] %(message)s'))
        logger.addHandler(handler)
    return logger


class DeadPeerDetectionTest(unittest.TestCase):
    """수신 연결이 조용히 죽었을 때 스스로 복구하는지 검증한다."""

    def setUp(self):
        # config.ini를 읽지 못하게 만들어 client.py의 기본값(localhost:7000/7001)을 쓰게 한다
        self.config_patch = mock.patch.object(configparser.ConfigParser, 'read', return_value=[])
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)

        template = np.zeros(19 * 50 + 20, dtype=np.int32)
        self.shm = shared_memory.SharedMemory(create=True, size=template.nbytes)
        self.addCleanup(self._cleanup_shm)
        self.template = template

        self.rx_server = SilentServer(RECEIVE_PORT)
        self.tx_server = DrainServer(TRANSMIT_PORT)
        self.rx_server.start()
        self.tx_server.start()
        self.addCleanup(self.rx_server.stop)
        self.addCleanup(self.tx_server.stop)

    def _cleanup_shm(self):
        try:
            self.shm.close()
            self.shm.unlink()
        except (OSError, FileNotFoundError):
            pass

    def test_silent_server_triggers_reconnect(self):
        """PING이 끊긴 뒤 일정 시간 안에 수신 소켓이 재연결을 시도해야 한다."""
        thread = client.TcpClientThread(
            tcp_queue=queue.Queue(1024),
            logging=make_logger(),
            GPIOADDR1=0x20,
            GPIOADDR2=0x21,
            socket_event=threading.Event(),
            i2c_semaphor=threading.Semaphore(1),
            MAXUNITBOARD=19,
            shm_name=self.shm.name,
            unit_np_shm=self.template,
            socket_send_queue=queue.Queue(4096),
            status_control_queue=queue.Queue(64),
        )
        thread.start()

        self.assertTrue(self.rx_server.accepted.wait(timeout=10),
                        '첫 수신 연결조차 수립되지 않았다')

        # 서버는 살아 있지만 PING을 더 보내지 않는다.
        # 죽은 연결을 감지한다면 두 번째 accept가 발생해야 한다.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if len(self.rx_server.accept_times) >= 2:
                break
            time.sleep(0.2)

        if len(self.rx_server.accept_times) >= 2:
            elapsed = self.rx_server.accept_times[1] - self.rx_server.accept_times[0]
            print(f'    -> 재연결까지 {elapsed:.1f}초')

        self.assertGreaterEqual(
            len(self.rx_server.accept_times), 2,
            '서버가 조용히 침묵했는데도 15초 안에 재연결하지 않았다 '
            '(무수신 감지 없음 → 좀비 연결로 영구 고착)')


class BackoffTest(unittest.TestCase):
    """재연결 대기 시간 계산이 '짧게 끊긴 연결'을 벌하는지 검증한다."""

    def test_stable_connection_resets_delay(self):
        delay = client.next_reconnect_delay(32, client.STABLE_CONNECTION_SECONDS + 1)
        self.assertEqual(delay, client.INITIAL_RECONNECT_DELAY)

    def test_short_connection_increases_delay(self):
        delay = client.next_reconnect_delay(1, 0.02)
        self.assertEqual(delay, 2)
        delay = client.next_reconnect_delay(delay, 0.02)
        self.assertEqual(delay, 4)

    def test_delay_is_capped(self):
        self.assertEqual(
            client.next_reconnect_delay(client.MAX_RECONNECT_DELAY, 0.0),
            client.MAX_RECONNECT_DELAY)

    def test_jitter_stays_in_range(self):
        for _ in range(200):
            value = client.apply_jitter(10)
            self.assertGreaterEqual(value, 10 * (1 - client.RECONNECT_JITTER_RATIO) - 1e-9)
            self.assertLessEqual(value, 10 * (1 + client.RECONNECT_JITTER_RATIO) + 1e-9)


class KeepaliveTest(unittest.TestCase):
    """소켓에 keepalive가 실제로 설정되는지 검증한다."""

    def test_keepalive_enabled(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(sock.close)
        client.configure_tcp_socket(sock, make_logger())
        self.assertNotEqual(
            sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 0,
            'SO_KEEPALIVE가 켜지지 않았다')

    def test_idle_timeout_exceeds_ping_interval(self):
        """무수신 판정 임계값은 PING 주기보다 충분히 커야 한다 (오탐 방지)."""
        self.assertGreaterEqual(
            client.RECEIVE_IDLE_TIMEOUT, client.SERVER_PING_INTERVAL * 3,
            '무수신 임계값이 PING 주기의 3배 미만이면 정상 연결도 끊게 된다')


if __name__ == '__main__':
    unittest.main(verbosity=2)
