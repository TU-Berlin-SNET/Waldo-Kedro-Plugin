"""
Custom Kedro Datasets
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

from kedro.config import ConfigLoader
from kedro.extras.datasets.pandas import SQLTableDataSet
import pandas as pd
import sqlalchemy
from .models import OutlierScore, Contexts
from .utilities import insert_context, get_session, Engine, partition_indexes
import logging
from io import StringIO


class OutlierScoreDataSet (SQLTableDataSet):
    """
    'OutlierScoreDataSet' loads data from a Waldo outlier_score PostgreSQL table and saves a pandas
    dataframe to Waldo 'outlier_score' PostgreSQL table. It handles the joining between the 'context' and
    the 'outlier_score' tables internally inside both '_load' and '_save' methods. For '_save' it has two modes
    of operation for batch insertion to the database, based on the selected value for the 'use_copy' argument.
    If 'use_copy' is False, it uses the '_save' implementation of parent SQLTableDataSet for the table insertion.
    If 'use_copy' is True, it uses the 'copy_from' implementation of psycopg2 for the table insertion.
    """
    def __init__(self, use_copy: bool = False) -> None:
        table_name = OutlierScore.__tablename__
        save_args = {
            "if_exists": "append",
            "index": False,
            "schema": "public",
            "method": "multi",
            "chunksize": 10000
        }
        conf_paths = ["conf/base"]
        conf_loader = ConfigLoader(conf_paths)
        credentials = conf_loader.get("credentials*")["postgres"]
        self.use_copy = use_copy
        super().__init__(table_name=table_name, credentials=credentials, save_args=save_args)

    def _load(self) -> pd.DataFrame:
        stmt = sqlalchemy.select(Contexts, OutlierScore).join_from(Contexts, OutlierScore)
        try:
            result_df = pd.read_sql(stmt, self._load_args["con"])
            return result_df
        except (sqlalchemy.exc.SQLAlchemyError, ValueError) as err:
            logging.error(err)
            return pd.DataFrame()

    def _save(self, data: pd.DataFrame) -> None:
        with get_session() as session:
            first = data.loc[0]
            new_context: Contexts = insert_context(session, first.at["run_id"], first.at["algorithm"], first.at["parameters"])

            data["context_id"] = new_context.id
            data.drop(["run_id", "algorithm", "parameters"], axis=1, inplace=True)

        if not self.use_copy:
            super()._save(data)
        else:
            pyscopg2_conn = Engine.raw_connection()
            self._copy_from_stringio(pyscopg2_conn, data)

    def _copy_from_stringio(self, conn, df):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        cursor = conn.cursor()
        for value in partition_indexes(df.size, self._save_args["chunksize"]):
            # save dataframe to an in memory buffer
            buffer = StringIO()
            df[value[0]:value[1]].to_csv(buffer, index=self._save_args["index"], header=False)
            buffer.seek(0)

            try:
                # Specifying the columns, to make sure that the order of columns in the dataframe and the db is the same
                cursor.copy_from(buffer, self._save_args["name"], sep=",", columns=['sample_id', 'score', 'prediction', 'context_id'])
                conn.commit()
            except Exception as error:
                logging.error(error)
                conn.rollback()
                cursor.close()
                exit(-1)

        cursor.close()
