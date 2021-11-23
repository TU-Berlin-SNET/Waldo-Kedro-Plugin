from setuptools import setup

setup(
    name="waldo_kedro_plugin",
    version="0.3.0",
    packages=["waldo_kedro_plugin"],
    install_requires=[
        "kedro-viz==3.14.0",
        "SQLAlchemy==1.4.23",
        "SQLAlchemy-Utils==0.37.8",
        "pandas==1.3.3",
        "scikit-learn~=0.24.1"
    ],
    entry_points={
        "kedro.hooks": ["waldo_kedro_plugin = waldo_kedro_plugin.plugin:hooks"]
    },
)
