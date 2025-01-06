import numpy as np

def calculate_relative_humidity(T: float, Td: float) -> float:
    """
    计算相对湿度（RH）。https://doi.org/10.1175/BAMS-86-2-225

    参数：
        T: 2米气温（摄氏度）
        Td: 2米露点温度（摄氏度）

    返回值：
        相对湿度 RH（%）。
    """
    numerator = np.exp(17.625 * Td / (243.04 + Td))
    denominator = np.exp(17.625 * T / (243.04 + T))
    RH = 100 * (numerator / denominator)
    return RH

def calculate_wet_bulb_temperature(T: float, RH: float) -> float:
    """
    计算湿球温度（Tw）。https://doi.org/10.1175/JAMC-D-11-0143.1

    参数：
        T: 2米气温（摄氏度）
        RH: 相对湿度（%）

    返回值：
        湿球温度 Tw（摄氏度）。
    """
    RH_sqrt = np.sqrt(RH + 8.313659)
    arctan1 = np.arctan(0.151977 * RH_sqrt)
    arctan2 = np.arctan(T + RH)
    arctan3 = np.arctan(RH - 1.676331)
    arctan4 = np.arctan(0.023101 * RH)

    Tw = (
        T * arctan1
        + arctan2
        - arctan3
        + 0.00391838 * (RH ** 1.5) * arctan4
        - 4.686035
    )
    return Tw

# 示例使用
if __name__ == "__main__":
    # 输入气温和露点温度（单位：摄氏度）
    T = 30.0  # 气温
    Td = 25.0  # 露点温度

    # 计算相对湿度
    RH = calculate_relative_humidity(T, Td)
    print(f"相对湿度（RH）：{RH:.2f}%")

    # 计算湿球温度
    Tw = calculate_wet_bulb_temperature(T, RH)
    print(f"湿球温度（Tw）：{Tw:.2f}°C")
