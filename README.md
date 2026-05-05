# PLC Monitor

工业PLC数据采集与监控工具，通过Modbus TCP协议读取PLC寄存器数据。

## 功能

- Modbus TCP协议数据采集
- SQLite数据持久化
- 断线自动重连
- 数据保留天数自动清理
- HTTP健康检查接口
- 采集日志记录

## 使用

1. 修改 plc_config_example.json 为 plc_config.json
2. pip install pymodbus
3. python monitor_plc.py

## 配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| scan_interval | 采集间隔(秒) | 2 |
| max_retry_delay | 重连最大退避(秒) | 300 |
| data_retention_days | 数据保留天数 | 30 |

## License

MIT
