import numpy as np
import pandas as pd
from src.transform import wind_chill, calculate_frostbite_risk, estimate_snowfall


def test_wind_chill_cold_wind():
    temp = np.array([-5.0])
    wind = np.array([30.0])
    expected = np.array([-13.0])
    result = wind_chill(temp, wind)
    assert np.isclose(result[0], expected[0], atol=0.2)



#hot temps/low wind => no chill effect
def test_wind_chill_mild():
    temp = np.array([12.0])
    wind = np.array([10.0])
    result = wind_chill(temp,wind)
    assert result[0] == 12.0



def test_frostbite_risk():
    wind_chill = np.array([-5, -20, -30, -40, -50])
    expected = [
        ">30 minutes (low risk)",
        "10-30 minutes",
        "5-10 minutes",
        "5 minutes or less",
        "<5 minutes"
    ]
    result = calculate_frostbite_risk(wind_chill)
    assert all(result == expected)


def test_snowfall_estimate():
    precip = np.array([1.0, 2.0, 0.0])
    temp = np.array([0.0, -15.0, 5.0])
    result = estimate_snowfall(precip, temp)
    assert np.isclose(result[0], 1.0)
    assert np.isclose(result[1], 3.0)
    assert result[2] == 0.0