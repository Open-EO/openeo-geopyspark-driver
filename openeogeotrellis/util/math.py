import math


def logarithmic_round(value: float, base=10, delta=0.01) -> float:
    """
    Round float value adaptively based on its order of magnitude,
    e.g. to build histograms with "relatively" sized bins."
    """
    if value == 0:
        return 0
    sign = 1.0 if value >= 0 else -1.0
    quantized = round(math.log(abs(value), base) / delta) * delta
    return sign * math.pow(base, quantized)
