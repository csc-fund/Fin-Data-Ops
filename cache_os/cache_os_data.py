
from pathlib import Path
import platform
from BBData import D
import shutil
sys_type = platform.system()
if sys_type == "Linux":
    start_point = "/mnt/z"
elif sys_type == "Windows":
    start_point = "Z:\\"
else:
    raise ValueError()


def cache_dset(dset_type, fn=1):
    DATA_ROOT = Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData" / dset_type / "data"
    CACHE_ROOT = Path(start_point) / "AI_prod" / "CentralProdRoutine" / "Data" / "RawData_os" / dset_type / f"tm{fn}"
    CACHE_ROOT.mkdir(parents=True, exist_ok=True) 

    dt = D.calendar(freq="Tday")[-(fn + 1)]
    dir_list = list(DATA_ROOT.glob("*"))
    for dset_path in dir_list:
        name = dset_path.name
        cache_path = CACHE_ROOT / name 
        cache_path.mkdir(exist_ok=True)
        if (dset_path / f"{dt}.f").exists():
            is_path = dset_path / f"{dt}.f"
            os_path = cache_path / f"{dt}.f"
            if not is_path.exists():
                alter = list(is_path.parent.glob("*.f"))
                if len(alter) == 1:
                    is_path = alter[0]
            shutil.copyfile(
                src=is_path,
                dst=os_path
            )


if __name__ == "__main__":
    cache_dset("wind", 1)
    cache_dset("suntime", 1)
    cache_dset("wind", 2)
    cache_dset("suntime", 2)