#  Copyright © 2021 Technische Unversität Berlin, Service-centric Networking (SNET) https://snet.tu-berlin.de/
#  Aljoscha Schulte, Christoph Schulthess, Uttam Dhakal, Zohaib Akhtar Khan
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

from setuptools import setup

setup(
    name="waldo_kedro_plugin",
    version="1.0",
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
