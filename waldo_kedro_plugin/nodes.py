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

iso_params = {
    "IsolationForest.n_estimators": 100,
    "IsolationForest.max_samples": "auto",
    "IsolationForest.contamination": "auto",
    "IsolationForest.max_features": 1.0,
    "IsolationForest.bootstrap": False,
    "IsolationForest.n_jobs": None,
    "IsolationForest.random_state": None,
    "IsolationForest.verbose": 0,
    "IsolationForest.warm_start": False,
}

ell_params = {
    "EllipticEnvelope.store_precision": True,
    "EllipticEnvelope.assume_centered": False,
    "EllipticEnvelope.support_fraction": None,
    "EllipticEnvelope.contamination": 0.1,
    "EllipticEnvelope.random_state": None,
}

local_params = {
    "LocalOutlierFactor.n_neighbors": 20,
    "LocalOutlierFactor.algorithm": "auto",
    "LocalOutlierFactor.leaf_size": 30,
    "LocalOutlierFactor.metric": "minkowski",
    "LocalOutlierFactor.p": 2,
    "LocalOutlierFactor.metric_params": None,
    "LocalOutlierFactor.contamination": "auto",
    "LocalOutlierFactor.novelty": False,
    "LocalOutlierFactor.n_jobs": None,
}


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
            try:
                iso_params.update(params["IsolationForest"])
            except KeyError:
                logging.info(
                    "No parameters for Isolation Forest found, using the default ones"
                )
            algo_obj: IsolationForest = IsolationForest(
                n_estimators=iso_params["IsolationForest.n_estimators"],
                max_samples=iso_params["IsolationForest.max_samples"],
                contamination=iso_params["IsolationForest.contamination"],
                max_features=iso_params["IsolationForest.max_features"],
                bootstrap=iso_params["IsolationForest.bootstrap"],
                n_jobs=iso_params["IsolationForest.n_jobs"],
                random_state=iso_params["IsolationForest.random_state"],
                verbose=iso_params["IsolationForest.verbose"],
                warm_start=iso_params["IsolationForest.warm_start"],
            ).fit(x)
            ols = -algo_obj.score_samples(x)
            prd = algo_obj.predict(x)
        elif algo == ADAlgorithms.EllipticEnvelope:
            try:
                ell_params.update(params["EllipticEnvelope"])
            except KeyError:
                logging.info(
                    "No parameters for Elliptic Envelope found, using the default ones"
                )
            algo_obj: EllipticEnvelope = EllipticEnvelope(
                store_precision=ell_params["EllipticEnvelope.store_precision"],
                assume_centered=ell_params["EllipticEnvelope.assume_centered"],
                support_fraction=ell_params["EllipticEnvelope.support_fraction"],
                contamination=ell_params["EllipticEnvelope.contamination"],
                random_state=ell_params["EllipticEnvelope.random_state"],
            ).fit(x)
            ols = -algo_obj.score_samples(x)
            prd = algo_obj.predict(x)
        elif algo == ADAlgorithms.LocalOutlierFactor:
            try:
                local_params.update(params["LocalOutlierFactor"])
            except KeyError:
                logging.info(
                    "No parameters for Local Outlier Factor found, using the default ones"
                )
            algo_obj: LocalOutlierFactor = LocalOutlierFactor(
                n_neighbors=local_params["LocalOutlierFactor.n_neighbors"],
                algorithm=local_params["LocalOutlierFactor.algorithm"],
                leaf_size=local_params["LocalOutlierFactor.leaf_size"],
                metric=local_params["LocalOutlierFactor.metric"],
                p=local_params["LocalOutlierFactor.p"],
                metric_params=local_params["LocalOutlierFactor.metric_params"],
                contamination=local_params["LocalOutlierFactor.contamination"],
                novelty=local_params["LocalOutlierFactor.novelty"],
                n_jobs=local_params["LocalOutlierFactor.n_jobs"],
            ).fit(x)
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
