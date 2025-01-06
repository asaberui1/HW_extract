import numpy as np
import xarray as xr
from tqdm import tqdm
import os

def calculate_daily_max_temp(data_path, start_year=1981, end_year=2010):
    """
    计算每日最高温度并保存为单独的 NetCDF 文件。

    参数：
        data_path: str, 存放ERA5数据的路径模板（例如"/data6/huangty/ERA5/ERA5_downloads/era5_<year>_<month>.nc"）。
        start_year: int, 起始年份。
        end_year: int, 结束年份。

    功能：
        读取逐小时的气温数据，计算每日最高温度，并将结果保存为以月份为单位的 NetCDF 文件。
    """
    total_years = end_year - start_year + 1
    total_months = total_years * 12

    with tqdm(total=total_months, desc="Calculating Tmax", unit="month") as pbar:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                file_path = data_path.replace("<year>", str(year)).replace("<month>", f"{month:02d}")

                try:
                    ds = xr.open_dataset(file_path)

                    # 提取2米气温
                    t2m_hourly = ds["t2m"] - 273.15  # 转换为摄氏度

                    # 计算每日最高2米气温
                    tm_daily = t2m_hourly.resample(valid_time="1D").max()

                    # 转换为xarray对象，保留原坐标信息
                    tm_da = xr.DataArray(
                        tm_daily, dims=tm_daily.dims, coords=tm_daily.coords, name="tm"
                    )

                    # 拼接输出路径
                    output_dir = os.path.dirname(data_path)
                    output_file = os.path.join(output_dir, f"tm_{year}_{month:02d}.nc")

                    # 保存每个月的日均湿球温度为单独文件
                    tm_da.to_netcdf(output_file)

                except FileNotFoundError:
                    print(f"File not found: {file_path}")

                pbar.update(1)

# 示例调用
if __name__ == "__main__":
    data_path = "/data6/huangty/ERA5/ERA5_downloads/era5_<year>_<month>.nc"

    # 保存结果
    calculate_daily_max_temp(data_path=data_path)
    print("每日最高温度计算完成，并保存到指定路径下的tm_<year>_<month>.nc")
