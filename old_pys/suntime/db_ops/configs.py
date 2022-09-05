from pathlib import Path
import platform

sys_type = platform.system()
if sys_type == "Linux":
    start_point = "/mnt/z"
elif sys_type == "Windows":
    start_point = "Z:"
else:
    raise ValueError()


TXT_ROOT = str(Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData" / "suntime" / "data")
LOGGER_ROOT = str(Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData" / "suntime" / "log")
JSON_PATH = str(Path(__file__).parent / "tables.json")
print(LOGGER_ROOT)