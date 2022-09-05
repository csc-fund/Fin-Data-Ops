from pathlib import Path
import platform

sys_type = platform.system()
if sys_type == "Linux":
    start_point = "/mnt/z"
elif sys_type == "Windows":
    start_point = "Z:"
elif sys_type == "Darwin":
    start_point = "/Users/mac/Downloads"
else:
    raise ValueError()

TXT_ROOT = str(Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData" / "wind" / "data")
LOGGER_ROOT = str(Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData" / "wind" / "log")
print(TXT_ROOT)
