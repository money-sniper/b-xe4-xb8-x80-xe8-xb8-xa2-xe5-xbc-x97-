#!/usr/bin/env python
# coding: utf-8

"""
短周期coint定义
"""

import warnings

import numpy as np
from numpy.linalg import LinAlgError
import pandas as pd
from statsmodels.tsa.tsatools import add_trend

from statsmodels.regression.linear_model import OLS, yule_walker
from statsmodels.tools.tools import Bunch, add_constant
from statsmodels.tools.sm_exceptions import (
    CollinearityWarning,)
from statsmodels.tools.validation import (
    array_like,
    bool_like,
    dict_like,
    float_like,
    int_like,
    string_like,
)
from statsmodels.tsa.adfvalues import mackinnoncrit, mackinnonp
from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.stattools import adfuller
SQRTEPS = np.sqrt(np.finfo(np.double).eps)

def coint(
    y0,
    y1,
    trend="c",
    method="aeg",
    maxlag=None,
    autolag="aic",
    return_results=None,
):
    y0 = array_like(y0, "y0")
    y1 = array_like(y1, "y1", ndim=2)
    trend = string_like(trend, "trend", options=("c", "nc", "ct", "ctt"))
    method = string_like(method, "method", options=("aeg",))
    maxlag = int_like(maxlag, "maxlag", optional=True)
    autolag = string_like(
        autolag, "autolag", optional=True, options=("aic", "bic", "t-stat")
    )
    return_results = bool_like(return_results, "return_results", optional=True)

    nobs, k_vars = y1.shape
    k_vars += 1  # add 1 for y0

    if trend == "nc":
        xx = y1
    else:
        xx = add_trend(y1, trend=trend, prepend=False)

    res_co = OLS(y0, xx).fit()

    if res_co.rsquared < 1 - 100 * SQRTEPS:
        res_adf = adfuller(
            res_co.resid, maxlag=maxlag, autolag=autolag, regression="c"
        )
    else:
        warnings.warn(
            "y0 and y1 are (almost) perfectly colinear."
            "Cointegration test is not reliable in this case.",
            CollinearityWarning,
        )
        # Edge case where series are too similar
        res_adf = (-np.inf,)

    # no constant or trend, see egranger in Stata and MacKinnon
    if trend == "nc":
        crit = [np.nan] * 3  # 2010 critical values not available
    else:
        crit = mackinnoncrit(N=k_vars, regression=trend, nobs=nobs - 1)
        #  nobs - 1, the -1 is to match egranger in Stata, I do not know why.
        #  TODO: check nobs or df = nobs - k

    pval_asy = mackinnonp(res_adf[0], regression=trend, N=k_vars)
    return res_adf[0], pval_asy, crit
