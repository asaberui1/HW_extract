import numpy as np
import xarray as xr
import pandas as pd
from tqdm import tqdm
import os

def calculate_wet_heatwave_threshold(input_path, output_path="./wbt_threshold.nc", start_year=1981, end_year=2010):
    """
    计算湿热复合热浪（Wet Heatwave）的阈值。

    参数：
        input_path: str, 输入湿球温度文件路径模板（例如"/data6/huangty/ERA5/ERA5_downloads/wbt_<year>_<month>.nc"）。
        output_path: str, 输出阈值文件路径（例如"./wbt_threshold.nc"）。
        start_year: int, 起始年份。
        end_year: int, 结束年份。

    功能：
        遍历输入的湿球温度数据，为 03 至 10 月的每一天计算前后 15 天滑动窗口的 90% 分位数，
        并将结果保存为 NetCDF 文件。
    """
    print("Loading data...")

    # 加载所有湿球温度数据
    datasets = []
    total_months = (end_year - start_year + 1) * 12

    with tqdm(total=total_months, desc="Loading data", unit="month") as pbar:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                file_path = input_path.replace("<year>", str(year)).replace("<month>", f"{month:02d}")
                try:
                    ds = xr.open_dataset(file_path)
                    datasets.append(ds["tw"])
                except FileNotFoundError:
                    print(f"File not found: {file_path}")
                pbar.update(1)

    # 合并为一个时间维度的 DataArray
    combined_data = xr.concat(datasets, dim="valid_time")

    with tqdm(total=245, desc="Calculating daily thresholds", unit="day") as pbar:
        all_25th = []  # 存储25%分位数
        all_75th = []  # 存储75%分位数
        all_90th = []  # 存储90%分位数

        for day_of_year in range(60, 305):  # 245天范围
            # 收集30年间该日期窗口的数据
            daily_window_data = []

            for year in range(start_year, end_year + 1):
                year_data = combined_data.sel(valid_time=(combined_data["valid_time"].dt.year == year))

                # 获取当前年份中有效日期范围
                current_dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
                if len(current_dates) == 366:  # 闰年
                    valid_days = list(range(1, 367))
                    day_start = day_of_year - 15
                    day_end = day_of_year + 15
                else:
                    valid_days = list(range(1, 366))
                    day_start = day_of_year - 16
                    day_end = day_of_year + 14

                # 处理跨年的情况
                if day_start < 1:
                    window_days = valid_days[day_start:] + valid_days[:day_end]
                elif day_end > len(valid_days):
                    window_days = valid_days[day_start:] + valid_days[:day_end - len(valid_days)]
                else:
                    window_days = valid_days[day_start:day_end + 1]

                window_data = year_data.sel(valid_time=year_data["valid_time"].dt.dayofyear.isin(window_days))
                daily_window_data.append(window_data)

            # 合并30年的窗口数据
            daily_window_data = xr.concat(daily_window_data, dim="valid_time")

            # 分别计算25%、75%、90%分位数
            daily_25th = np.nanpercentile(daily_window_data, 25, axis=0)
            daily_75th = np.nanpercentile(daily_window_data, 75, axis=0)
            daily_90th = np.nanpercentile(daily_window_data, 90, axis=0)

            all_25th.append(daily_25th)
            all_75th.append(daily_75th)
            all_90th.append(daily_90th)
            pbar.update(1)

    print("Saving threshold data...")

    # 创建 xarray.Dataset 存储三种分位数
    threshold_ds = xr.Dataset(
        {
            "wbt_25th": (["day_of_year", "latitude", "longitude"], np.stack(all_25th, axis=0)),
            "wbt_75th": (["day_of_year", "latitude", "longitude"], np.stack(all_75th, axis=0)),
            "wbt_90th": (["day_of_year", "latitude", "longitude"], np.stack(all_90th, axis=0)),
        },
        coords={
            "day_of_year": range(60, 305),
            "latitude": combined_data["latitude"],
            "longitude": combined_data["longitude"],
        },
    )

    # 保存到 NetCDF 文件
    threshold_ds.to_netcdf(output_path)

    print(f"Thresholds (25th, 75th, 90th) saved to {output_path}")



# 示例调用
if __name__ == "__main__":
    input_path = "/data6/huangty/ERA5/ERA5_downloads/wbt_<year>_<month>.nc"

    calculate_wet_heatwave_threshold(input_path=input_path)

    print("湿热复合热浪阈值计算完成，并保存到指定文件。")

