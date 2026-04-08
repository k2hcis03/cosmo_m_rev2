# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language & Communication

- All conversation and responses must be in **Korean (한국어)**.
- Git commit messages must be written in Korean.

## Git Commit Message Format

```
[타입] 변경 내용 요약

- 상세 변경 사항 1
- 상세 변경 사항 2
```

Commit types: `[기능]` `[수정]` `[개선]` `[리팩토링]` `[문서]` `[스타일]` `[테스트]` `[설정]` `[의존성]`

## Code Style

- Follow PEP 8. Variables and functions: `snake_case`. Classes: `PascalCase`.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running the System

```bash
# Main control system (requires sudo for CAN/I2C hardware access)
sudo python3 source/main.py

# Firmware upload REST API (separate service)
cd source && uvicorn app:app --host 192.168.100.5 --port 9002

# Interactive test/debug clients
python3 source/json_test.py
python3 source/jsonserver_receive.py
python3 source/jsonserver_send.py
```

Logs are written to `log/{timestamp}/debug.log` (rotating, 1MB max, 5 backups).

## Architecture Overview

Cosmo-M is a **fermentation/production tank control system** running on an **industrial Raspberry Pi CM4 (Compute Module 4)**. It orchestrates multiple embedded unit boards (UnitBoard) over CAN-FD, manages sensors and motors, runs PID temperature control, and communicates with a management PC over JSON/TCP.

### Communication Stack

```
Main PC (JSON/TCP)
    ↓
TcpServerThread  ←→  I2C GPIO (relay control)
    ↓
CosmoMain (command router, main.py)
    ├── CanFDReceive thread   (CAN bus RX, IDs 0x100–0x11F)
    ├── CanFDTransmitte thread (CAN bus TX)
    └── UnitBoard processes  (one per tank, ProcessPoolExecutor)
         └── UnitBoardCanFdReceive (parses CAN responses → shared memory)
```

### Key Modules

| File | Role |
|------|------|
| [source/main.py](source/main.py) | Entry point. Initializes CAN bus (1 Mbps / 4 Mbps data), I2C buses (0x20, 0x21), shared memory, spawns all threads/processes, routes commands by TANK_ID. |
| [source/canfd.py](source/canfd.py) | CAN-FD RX/TX threads. Recovers from bus errors by reinitializing the interface. Distributes messages to per-unit queues. |
| [source/unitboard.py](source/unitboard.py) | Core unit board logic. `UnitBoardCanFdReceive` parses CAN frames → shared memory. `UnitBoard.unit_process()` handles all commands (STATE, REF, SET_MOTOR, SET_GPIO, FIRMWARE_UPDATE, etc.). |
| [source/client.py](source/client.py) | TCP server thread. Parses multi-packet JSON, routes to CosmoMain, sends sensor/status responses. Also handles I2C relay writes. |
| [source/constdefine.py](source/constdefine.py) | All constants: shared memory offsets, command codes, error codes (10–29), default PID params, timer intervals. **Always consult this before using magic numbers.** |
| [source/pid_controller.py](source/pid_controller.py) | `PID_COSMO_M` wraps `simple_pid.PID` with integral windup clipping and setpoint management. |
| [source/logger.py](source/logger.py) | Queue-based multi-process-safe logger. |
| [source/app.py](source/app.py) | Litestar REST API — `POST /upload` saves firmware binaries to `firmware/`. |

### Inter-Process Communication

- **Shared memory**: NumPy array, 50 bytes per unit board. Layout defined in `constdefine.py` (offsets for PID state, ADC channels ×8, temperatures ×8, GPIO, RPM, load cell, Brix/CO2/pH, error codes).
- **Queues**: Commands flow Main → UnitBoard; responses flow UnitBoard → TcpServerThread.
- **Semaphores**: Protect shared memory critical sections.

### Configuration

`config/config.ini` (INI format) has two sections:
- `[common]`: MAX_UNITBOARD count, HOST/PORT, I2C GPIO addresses, tank type counts (fermentation/blending/production/aging/chiller), shared memory size, poll interval.
- `[unit_board0..N]`: Per-unit TANK_ID, motor/sensor IDs, ADC temperature calibration (slope + offset per channel), relay mappings, RPM limits, external sensor ModBUS addresses.
