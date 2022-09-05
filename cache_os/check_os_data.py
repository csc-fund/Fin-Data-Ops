from pathlib import Path
import platform
from BBData import D
import shutil
import pandas as pd

sys_type = platform.system()
if sys_type == "Linux":
    start_point = Path("/mnt/z/AI_prod/CentralProdRoutine/Data")
elif sys_type == "Windows":
    start_point = Path(r"Z:\AI_prod\CentralProdRoutine\Data")
else:
    raise ValueError()


class CheckOSData:

    def __init__(self, table_name, dset_type="wind", fn=1):
        self.table_name = table_name
        self.fn = fn
        self.dset_type = dset_type

    @staticmethod
    def list_dset(dset_type):
        path_dsets = (start_point / "RawData" / dset_type / "data").glob("*")
        path_dsets = [i.name for i in path_dsets]
        return path_dsets

    @staticmethod
    def records_diff(df_os, df_is):
        row_disappear = list(set(df_os.index) - set(df_is.index))
        row_new = list(set(df_is.index) - set(df_os.index))
        return row_disappear, row_new

    @staticmethod
    def value_diff(df_os: pd.DataFrame, df_is: pd.DataFrame) -> pd.DataFrame:
        """
        :param df_os: indexed with the OBJECT_ID
        :param df_is: indexed with the OBJECT_ID
        """
        row_disappear, row_new = CheckOSData.records_diff(df_os, df_is)
        df_is = df_is.reindex(df_os.index)
        check_dict = {}

        # check nan diff
        for col in df_is.columns:
            check_dict[col] = {}
            check_dict[col]["osnan_isval"] = (df_os[col].isna() & df_is[col].notna()).sum()
            check_dict[col]["isnan_osval"] = (df_is[col].isna() & df_os[col].notna()).sum()

        # object type columns diff
        str_cols = df_is.select_dtypes(include=["object", "datetime"]).columns
        for obj_col in str_cols:
            diff = df_is[obj_col].fillna(0) != df_os[obj_col].fillna(0)
            check_dict[obj_col]["object_val_differ"] = diff.sum()
            check_dict[obj_col]["diff_pbject"] = df_is.loc[diff].index.values.tolist()
            check_dict[obj_col]["dtype"] = "object_or_datetime"

        # numeric type columns diff
        num_cols = df_is.select_dtypes(exclude=["object", "datetime"]).columns
        for obj_col in num_cols:
            diff = df_is[obj_col].fillna(0) - df_os[obj_col].fillna(0)
            check_dict[obj_col]["numeric_val_differ"] = diff.abs().mean()
            std = 1
            if not diff.empty:
                std = max(diff.std(), 1)
            check_dict[obj_col]["numeric_val_differ"] /= std
            check_dict[obj_col]["diff_pbject"] = df_is.loc[diff != 0].index.values.tolist()
            check_dict[obj_col]["dtype"] = "numeric"

        check_dict["row_disappear"] = {}
        check_dict["row_new"] = {}
        check_dict["row_disappear"]["diff_pbject"] = row_disappear
        check_dict["row_new"]["diff_pbject"] = row_new
        return pd.DataFrame(check_dict).T

    @property
    def _url_os(self):
        dt = D.calendar(freq="Tday")[-(self.fn + 2)]
        os_path = start_point / "RawData_os" / self.dset_type / f"tm{self.fn}" / self.table_name / f"{dt}.f"
        assert os_path.exists(), "os not exis"
        print(str(os_path))
        return os_path

    @property
    def df_os(self):
        if self.dset_type == "wind":
            return pd.read_feather(self._url_os).set_index("OBJECT_ID")
        else:
            return pd.read_feather(self._url_os).set_index("ID")

    @property
    def _url_is(self):
        dt = D.calendar(freq="Tday")[-(self.fn + 2)]
        is_path = start_point / "RawData" / self.dset_type / "data" / self.table_name / f"{dt}.f"
        if not is_path.exists():
            exist_file = list(is_path.parent.glob("*.f"))
            assert len(exist_file) == 1, "is not exist"
            return exist_file[0]
        return is_path

    @property
    def _url_log(self):
        dt = D.calendar(freq="Tday")[-(self.fn + 2)]
        log_path = start_point / "RawData_os" / self.dset_type / f"tm{self.fn}_log" / self.table_name / f"{dt}.csv"
        log_path.parent.mkdir(exist_ok=True, parents=True)
        assert log_path.parent.exists()
        return log_path

    @property
    def df_is(self):
        if self.dset_type == "wind":
            return pd.read_feather(self._url_is).set_index("OBJECT_ID")
        else:
            return pd.read_feather(self._url_is).set_index("ID")

    def check_data(self):
        try:
            result = self.value_diff(self.df_os, self.df_is)
        except Exception as e:
            result = pd.Series({"Error": str(e), "trace_back": str(e.__traceback__.tb_frame)})
            print(self.table_name, result)
        result.to_csv(str(self._url_log))
        return result


if __name__ == '__main__':
    for table_name in CheckOSData.list_dset("wind"):
        CheckOSData(table_name, "wind", 1).check_data()
        CheckOSData(table_name, "wind", 2).check_data()
    for table_name in CheckOSData.list_dset("suntime"):
        CheckOSData(table_name, "suntime", 1).check_data()
        CheckOSData(table_name, "suntime", 2).check_data()