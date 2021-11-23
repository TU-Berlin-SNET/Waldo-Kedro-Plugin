"""
Generic Kedro Nodes
"""

import logging
import json
from sklearn.ensemble import IsolationForest
from sklearn.covariance import EllipticEnvelope
from sklearn.neighbors import LocalOutlierFactor
from .plugin import hooks
import pandas as pd
from .views import ADAlgorithms, create_samples_os_view


def isolation_forest(data: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Calculate outlier score using *isolation forest* algorithm for the input dataframe, on the list of columns specified
    inside ``params.cols``.

    :param data: input dataframe
    :param params: parameters for the anomaly detection model

    :return: a dataframes with anomaly detection scores and predictions
    """

    create_samples_os_view(ADAlgorithms.IsolationForest, params)
    return outlier_score(ADAlgorithms.IsolationForest, data, params)


def elliptic_envelope(data: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Calculate outlier score using *elliptic envelope* algorithm for the input dataframe, on the list of columns
    specified inside ``params.cols``.

    :param data: input dataframe
    :param params: parameters for the anomaly detection model

    :return: a dataframes with anomaly detection scores and predictions
    """

    create_samples_os_view(ADAlgorithms.EllipticEnvelope, params)
    return outlier_score(ADAlgorithms.EllipticEnvelope, data, params)


def local_outlier_factor(data: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Calculate outlier score using *local outlier factor* algorithm for the input dataframe,
    on the list of columns specified inside ``params.cols``.

    :param data: input dataframe
    :param params: parameters for the anomaly detection model

    :return: a dataframes with anomaly detection scores and predictions
    """

    create_samples_os_view(ADAlgorithms.LocalOutlierFactor, params)
    return outlier_score(ADAlgorithms.LocalOutlierFactor, data, params)


def outlier_score(algo: ADAlgorithms, data: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    calculate outlier score using the algorithm that has been specified by one of the wrapper functions
    (e.g. isolation forest, eliptic curve or local outlier factor).

    :param algo: algorithm to be used for the outlier detection.
    :param data: input dataframe.
    :param params: module specific parameters.
    :return: a dataframe with os metric (outlier score, prediction, used algorithm and parameters)
    """
    cols = params["cols"]
    x = data[cols].to_numpy()
    try:
        if algo == ADAlgorithms.IsolationForest:
            algo_obj: IsolationForest = IsolationForest(n_jobs=-1).fit(x)
            ols = -algo_obj.score_samples(x)
            prd = algo_obj.predict(x)
        elif algo == ADAlgorithms.EllipticEnvelope:
            algo_obj: EllipticEnvelope = EllipticEnvelope().fit(x)
            ols = -algo_obj.score_samples(x)
            prd = algo_obj.predict(x)
        elif algo == ADAlgorithms.LocalOutlierFactor:
            algo_obj: LocalOutlierFactor = LocalOutlierFactor(n_jobs=-1).fit(x)
            ols = -algo_obj.negative_outlier_factor_
            prd = algo_obj.fit_predict(x)
    except MemoryError as e:
        logging.error(e)
        raise Exception("Ran out of memory") from e
    except Exception as e:
        logging.error(e)
        raise Exception(f"Could not run {algo.value}") from e

    # 1 for inliers, -1 for outliers.
    predictions: list[bool] = [x == -1 for x in prd]

    os_df: pd.DataFrame = pd.DataFrame()
    os_df["sample_id"] = data["id"]
    os_df["run_id"] = hooks.trace_id
    os_df["score"] = ols
    os_df["algorithm"] = algo.value
    os_df["parameters"] = json.dumps(params)
    os_df["prediction"] = predictions

    return os_df
