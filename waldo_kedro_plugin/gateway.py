"""
Gateways provide a mechanism to validate the input and output of a Kedro node.
There are two types of gateways:

**Predefined**
    These gateways are provided by the waldo plugin.
**Custom**
    A gateway with custom business logic specific to a use case.

Gateways have full access to the inputs and outputs of the node they are applied to.
A gateway does not have any state.

**Steps to add a gateway to a node:**

1. Instantiate Gateway in src/$projectname/hooks.py:register_pipelines()
2. Opt: add custom tag as constructor parameter, otherwise the classname is used
3. Add tag to node in pipeline

The gateway logic is now automatically applied before and after a tagged node is called.
"""

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

import logging
from typing import Dict, Any
from pandas import DataFrame
from kedro.pipeline.node import Node
from pandas.api.types import is_numeric_dtype

waldo_gateway_registry = []


class WaldoGateway(object):
    """
    Waldo gateway base class. To be extended by specific implementations.

    :param tag: Additional tags to activate this gateway (class name is used by default)
    """

    def __init__(self, tag=None):
        waldo_gateway_registry.append(self)
        self.tags = {self.__class__.__name__}
        if tag:
            self.tags.add(tag)
        super().__init__()

    def validate_input(self, node: Node, inputs: Dict[str, Any]):
        """
        Stub to be overwritten by gateway implementations.

        :param node: Node to process the input data.
        :param inputs: input data to validate.
        """
        pass

    def validate_output(self, node: Node, outputs: Dict[str, Any]):
        """
        Stub to be overwritten by gateway implementations.

        :param node: Node that produced the output.
        :param outputs: Output data to validate.
        """
        pass


class LoggingGateway(WaldoGateway):
    """Example gateway implementation that does nothing much but print a log message."""

    def __init__(self):
        super().__init__()

    def validate_input(self, node: Node, inputs: Dict[str, Any]):
        """Log message."""
        logging.info("Input validated")

    def validate_output(self, node: Node, outputs: Dict[str, Any]):
        """Log message."""
        logging.info("Output validated")


class NumericInputValidatorGateway(WaldoGateway):
    """Example gateway implementation that checks if all the input parameters have numeric data type"""

    def validate_input(self, node: Node, inputs: Dict[str, Any]):
        """
        validate the input data
        :param:
            node: A kedro node
            inputs: Pandas Dataframe
        :return:
            True: If all the parameters sent to a node are numeric
        """

        valid: bool = True
        dataset: DataFrame = DataFrame()
        dataset_name: str = ""
        params: list = []
        # fills the dataset and the params from the inputs dictionary
        for key, value in inputs.items():
            if isinstance(value, DataFrame):
                dataset_name = key
                dataset = value
            elif key.startswith("params:") and isinstance(value, dict) and "cols" in value:
                params = value["cols"]

        # checks if the types of the columns defined in params, are numeric inside the dataset dataframes
        if not dataset.empty and params:
            for param in params:
                if not is_numeric_dtype(dataset.dtypes[param]):
                    valid = False
                    break

            if valid:
                logging.info(f"All the parameters {params} are numeric inside the input dataset {dataset_name}")
                return valid
        else:
            logging.error(f"Either dataset or the parameters are missing in the node input")
            exit(-1)

        logging.error(f"All the parameters {params} inside the input dataset {dataset_name} are not numeric")
        exit(-1)

    def validate_output(self, node: Node, outputs: Dict[str, Any]):
        """Empty"""


def validate_node_input(node: Node, inputs: Dict[str, Any]):
    """
    Execute input validation function for each gateway registered via its `__init__` method.

    :param node: The node that triggered this hook function.
    :param inputs: Dictionary of inputs to be validated by the gateway (if the node's tags match)
    """
    for gateway in waldo_gateway_registry:
        if gateway.tags.intersection(node.tags):
            gateway.validate_input(node, inputs)


def validate_node_output(node: Node, outputs: Dict[str, Any]):
    """
    Execute output validation function for each gateway registered via its `__init__` method.

    :param node: The node that triggered this hook function.
    :param outputs: Dictionary of outputs to be validated by the gateway (if the node's tags match)
    """
    for gateway in waldo_gateway_registry:
        if gateway.tags.intersection(node.tags):
            gateway.validate_output(node, outputs)
