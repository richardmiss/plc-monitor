#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json
import os
import sys
import sqlite3
import logging
import threading
import struct
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

from pymodbus.client import ModbusTcpClient

CONFIG_FILE = "plc_config.json"
DB_FILE = "plc_data.db"
LOG_FILE = "plc_monitor.log"
PID_FILE = "plc_monitor.pid"
HEALTH_PORT = 8080

# logging config
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/{LOG_FILE}", encoding='utf-8')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

# health status
health_status = {"status": "starting", "plcs": {}, "total_collections": 0}
health_lock = threading.Lock()


# database operations
def init_database(retention_days=30):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS plc_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            plc_name TEXT NOT NULL,
            variable_name TEXT NOT NULL,
            value REAL,
            data_type TEXT,
            quality TEXT DEFAULT 'GOOD'
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON plc_data(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_plc_name ON plc_data(plc_name)')
    conn.commit()
    conn.close()
    logger.info("Database initialized")
    clean_old_data(retention_days)


def clean_old_data(days=30):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        c.execute("DELETE FROM plc_data WHERE timestamp < ?", (cutoff,))
        deleted = c.rowcount
        conn.commit()
        if deleted > 0:
            logger.info(f"Cleaned {deleted} old records")
    except Exception as e:
        logger.error(f"Clean failed: {e}")
    finally:
        conn.close()


def save_to_database(plc_name, var_name, value, data_type, quality, timestamp):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''
            INSERT INTO plc_data (timestamp, plc_name, variable_name, value, data_type, quality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (timestamp, plc_name, var_name, value, data_type, quality))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Save failed: {e}")


# health check http server
class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            with health_lock:
                self.wfile.write(json.dumps(health_status).encode())


def start_health_server():
    try:
        server = HTTPServer(('127.0.0.1', HEALTH_PORT), HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        logger.info(f"Health server started: http://127.0.0.1:{HEALTH_PORT}/health")
    except OSError as e:
        logger.error(f"Health server failed: {e}")


# modbus helpers
def read_float32(client, address, count=2):
    """Read 32-bit float from modbus holding registers (2 consecutive 16-bit registers)"""
    try:
        result = client.read_holding_registers(address, count, unit=1)
        if result.isError():
            return None
        regs = result.registers
        # big-endian float: first register is high word
        if len(regs) >= 2:
            combined = (regs[0] << 16) | regs[1]
            return struct.unpack('>f', struct.pack('>I', combined))[0]
    except Exception as e:
        logger.debug(f"Float read error at {address}: {e}")
    return None


def read_word(client, address):
    """Read 16-bit word from modbus holding registers"""
    try:
        result = client.read_holding_registers(address, 1, unit=1)
        if result.isError():
            return None
        return result.registers[0]
    except Exception as e:
        logger.debug(f"Word read error at {address}: {e}")
    return None


# plc connection with backoff retry
class PlcConnection:
    def __init__(self, name, host, port, max_delay=300):
        self.name = name
        self.host = host
        self.port = port
        self.client = None
        self.retry_delay = 1
        self.max_delay = max_delay
        self.connected = False
        self.last_data = {"vw": {}, "vd": {}}

    def connect(self):
        try:
            self.client = ModbusTcpClient(self.host, port=self.port, timeout=5)
            if self.client.connect():
                self.connected = True
                self.retry_delay = 1
                return True
        except Exception as e:
            logger.warning(f"{self.name} connect failed: {e}")
        self.connected = False
        return False

    def disconnect(self):
        try:
            if self.client:
                self.client.close()
        except:
            pass
        self.connected = False

    def read_data(self, config):
        """Read VW and VD data according to config"""
        vw_data = {}
        vd_data = {}
        
        # read words (VW addresses)
        vw_start = config.get("vw_start", 0)
        vw_end = config.get("vw_end", 100)
        vw_batch = config.get("batch_size_vw", 30)
        
        for addr in range(vw_start, vw_end, vw_batch):
            count = min(vw_batch, vw_end - addr)
            try:
                result = self.client.read_holding_registers(addr, count, unit=1)
                if not result.isError():
                    for i in range(count):
                        val = result.registers[i]
                        if val != 0:
                            vw_data[addr + i] = val
            except Exception as e:
                logger.debug(f"VW batch read error at {addr}: {e}")
            time.sleep(config.get("scan_delay", 0.05))

        # read floats (VD addresses)
        vd_start = config.get("vd_start", 0)
        vd_end = config.get("vd_end", 100)
        
        for addr in range(vd_start, vd_end):
            val = read_float32(self.client, addr)
            if val is not None and abs(val) >= 1e-6:
                vd_data[addr] = val
            time.sleep(config.get("scan_delay", 0.05))

        return vw_data, vd_data

    def read_with_retry(self, config):
        """Read with automatic reconnection using exponential backoff"""
        try:
            if not self.connected or not self.client:
                if not self.connect():
                    return self.last_data["vw"], self.last_data["vd"], "STALE"

            vw_data, vd_data = self.read_data(config)
            
            if vw_data or vd_data:
                self.last_data["vw"] = vw_data
                self.last_data["vd"] = vd_data
                self.retry_delay = 1
                return vw_data, vd_data, "GOOD"
            else:
                raise Exception("No data returned")
                
        except Exception as e:
            logger.warning(f"{self.name} read error: {e}")
            delay = min(self.retry_delay, self.max_delay)
            logger.info(f"{self.name} reconnect in {delay}s")
            time.sleep(delay)
            self.retry_delay = min(self.retry_delay * 2, self.max_delay)
            self.disconnect()
            
            if self.connect():
                logger.info(f"{self.name} reconnected")
                self.retry_delay = 1
                return self.read_with_retry(config)
            else:
                logger.error(f"{self.name} reconnect failed")
                return self.last_data["vw"], self.last_data["vd"], "STALE"


def load_config():
    """Load configuration from JSON file"""
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file {CONFIG_FILE} not found")
        return None
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Config load failed: {e}")
        return None


def main():
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    logger.info(f"PLC Monitor started, PID: {os.getpid()}")

    config = load_config()
    if not config:
        logger.error("Config error, exiting")
        return

    # load global settings
    global_cfg = config.get("global", {})
    scan_interval = global_cfg.get("scan_interval", 2)
    max_retry_delay = global_cfg.get("max_retry_delay", 300)
    data_retention_days = global_cfg.get("data_retention_days", 30)
    log_dir = global_cfg.get("log_dir", "logs")
    
    os.makedirs(log_dir, exist_ok=True)

    init_database(data_retention_days)
    start_health_server()

    # setup plc connections
    plcs = config.get("plcs", [])
    if not plcs:
        logger.error("No PLC configured")
        return

    connections = []
    for plc in plcs:
        if not plc.get("enabled", True):
            continue
        name = plc.get("name", "unknown")
        ip = plc.get("ip")
        port = plc.get("port", 502)
        
        conn = PlcConnection(name, ip, port, max_retry_delay)
        if conn.connect():
            logger.info(f"Connected to {name} ({ip}:{port})")
        else:
            logger.error(f"Failed to connect to {name} ({ip}:{port})")
        connections.append((conn, plc))

    if not connections:
        logger.error("No PLC connections available, exiting")
        return

    # main loop
    try:
        while True:
            start_time = time.time()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Cycle start: {timestamp}")
            
            success_count = 0
            
            # sequential read for each PLC
            for conn, plc in connections:
                vw_data, vd_data, quality = conn.read_with_retry(plc.get("modbus", {}))
                
                with health_lock:
                    health_status["plcs"][conn.name] = {
                        "connected": quality == "GOOD",
                        "last_update": timestamp
                    }
                
                if quality == "GOOD":
                    success_count += 1
                    logger.info(f"[{conn.name}] (GOOD)")
                else:
                    logger.warning(f"[{conn.name}] (STALE - using cached)")
                
                # log and save data
                for addr, val in sorted(vw_data.items()):
                    if val != 0:
                        var_name = f"VW{addr}"
                        logger.info(f"  {var_name} = {val}")
                        save_to_database(conn.name, var_name, val, "int16", quality, timestamp)
                
                for addr, val in sorted(vd_data.items()):
                    if abs(val) >= 1e-6:
                        var_name = f"VD{addr}"
                        logger.info(f"  {var_name} = {val}")
                        save_to_database(conn.name, var_name, val, "float32", quality, timestamp)
                
                # JSON backup
                safe_name = conn.name.replace(" ", "_").replace("/", "_")
                log_file = os.path.join(log_dir, f"{safe_name}_{datetime.now().strftime('%Y-%m-%d')}.txt")
                log_entry = {
                    "time": timestamp,
                    "quality": quality,
                    "vw": {str(k): v for k, v in vw_data.items() if v != 0},
                    "vd": {str(k): v for k, v in vd_data.items() if abs(v) >= 1e-6}
                }
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

            # update health
            with health_lock:
                health_status["status"] = "running"
                health_status["total_collections"] += 1

            # interval control
            elapsed = time.time() - start_time
            remaining = scan_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
            else:
                logger.warning(f"Cycle took {elapsed:.2f}s, exceeds interval {scan_interval}s")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        for conn, _ in connections:
            conn.disconnect()
        logger.info("All connections closed")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)


if __name__ == "__main__":
    main()
