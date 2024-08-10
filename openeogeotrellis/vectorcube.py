import logging
from pathlib import Path
from typing import Optional, Callable

import pandas
import pandas as pd

import openeo.udf
import openeo.udf.run_code
from openeo.util import TimingLogger
from openeo_driver.datacube import SupportsRunUdf
from openeo_driver.save_result import AggregatePolygonResultCSV, JSONResult
from openeo_driver.utils import EvalEnv
from openeogeotrellis.utils import temp_csv_dir

_log = logging.getLogger(__name__)


class AggregateSpatialResultCSV(AggregatePolygonResultCSV, SupportsRunUdf):
    """
    `aggregate_spatial` result (with time dimension)
    """
    # TODO #71 #114 EP-3981 port this to proper vector cube support
    # TODO: move AggregatePolygonResultCSV to openeo-geopyspark-driver and merge with AggregateSpatialResultCSV
    # TODO: eliminate/simplify SaveResult class hierarchy instead of adding to it (https://github.com/Open-EO/openeo-python-driver/issues/149)
    # TODO: Move loading from CSV to a factory class method and allow creating an instance from something else too (inline data, netcdf, ...)

    def supports_udf(self, udf: str, runtime:str="Python") -> bool:
        udf_globals = openeo.udf.run_code.load_module_from_string(code=udf)
        return any(
            name in udf_globals
            for name in [
                "udf_apply_udf_data",
                "udf_apply_feature_dataframe",
            ]
        )

    def run_udf(self, udf: str, *, runtime: str = "Python", context: Optional[dict] = None, env: EvalEnv):
        # TODO: leverage `runtime` argument?
        udf_globals = openeo.udf.run_code.load_module_from_string(code=udf)

        # TODO: Port this UDF detection to openeo.udf.run_code?
        if "udf_apply_udf_data" in udf_globals:
            udf_function = udf_globals["udf_apply_udf_data"]
            callback = self._get_run_udf_udf_data_callback(
                udf_function=udf_function, context=context
            )
        elif "udf_apply_feature_dataframe" in udf_globals:
            udf_function = udf_globals["udf_apply_feature_dataframe"]
            callback = self._get_run_udf_pandas_callback(
                udf_function=udf_function, context=context
            )
        else:
            raise openeo.udf.OpenEoUdfException("No UDF found")

        csv_paths = list(Path(self._csv_dir).glob("*.csv"))
        _log.info(
            f"{type(self).__name__} run_udf with {self._csv_dir=} ({len(csv_paths)=})"
        )
        import pyspark.pandas
        csv_df = pyspark.pandas.read_csv([f"file://{p}" for p in csv_paths])

        processed_df = csv_df.groupby("feature_index").apply(callback).reset_index()

        output_dir = temp_csv_dir(message=f"{type(self).__name__}.run_udf output")
        with TimingLogger(logger=_log, title=f"Dump {processed_df=} to {output_dir=}"):
            processed_df.to_csv(f"file://{output_dir}")

        # Read CSV result(s) as a single pandas DataFrame
        # TODO: make "feature_index" the real index, instead of generic autoincrement index?
        result_df = pandas.concat(
            (pd.read_csv(p) for p in Path(output_dir).glob("*.csv")),
            ignore_index=True,
        )
        # TODO: return real vector cube instead of adhoc jsonifying the data here
        return JSONResult(data=result_df.to_dict("split"))

    @staticmethod
    def _get_run_udf_pandas_callback(
        udf_function: Callable, context: Optional[dict] = None
    ) -> Callable:
        """
        Build `pyspark.pandas.groupby.GroupBy.apply` callback

        :param udf_function: UDF that takes apandas.DataFrame and returns one of:
            - pandas.DataFrame
            - pandas.Series
            - scalar (float, str)
        :param context:
        :return:
        """

        def callback(data: pandas.DataFrame):
            # Get current feature index and drop whole column
            feature_index = data["feature_index"].iloc[0]
            feature_data = data.drop("feature_index", axis=1).set_index("date")
            # TODO: also pass feature_index to udf?
            processed = udf_function(feature_data)

            # Post-process UDF output
            if isinstance(processed, (int, float, str)):
                processed = pandas.Series([processed])
            elif isinstance(processed, dict):
                _log.warning(
                    f"Experimental UDF return data support: auto-convert dict to DataFrame "
                )
                processed = pandas.DataFrame.from_dict(processed, orient="index")
            elif isinstance(processed, list):
                _log.warning(
                    f"Experimental UDF return data support: auto-convert list to DataFrame "
                )
                processed = pandas.DataFrame(processed)

            if not isinstance(processed, (pandas.Series, pandas.DataFrame)):
                raise openeo.udf.OpenEoUdfException(
                    f"Failed to convert UDF return type to pandas Series/DataFrame: {type(processed)}"
                )
            return processed

        return callback

    @staticmethod
    def _get_run_udf_udf_data_callback(
        udf_function: Callable, context: Optional[dict] = None
    ) -> Callable:
        """
        Build `pyspark.pandas.groupby.GroupBy.apply` callback

        :param udf_function: UDF that takes an openeo.udf.UdfData object and returns one of:
            - openeo.udf.UdfData
            - scalar (float, str)
            - dict (to be auto-converted with pandas.DataFrame.from_dict)
            - list (to be auto-converted with pandas.DataFrame()
            - pandas.DataFrame or pandas.Series
        :param context:
        :return:
        """
        def callback(data: pandas.DataFrame):
            # Get current feature index and drop whole column
            feature_index = data["feature_index"].iloc[0]
            feature_data = data.drop("feature_index", axis=1)
            # TODO: We assume here that the `date` column already has parsed dates (naive, without timezone info).
            #       At the moment `pyspark.pandas.read_csv` seems to parse dates automatically
            #       (as pandas timestamp) even-though the docs states otherwise
            #       also see https://issues.apache.org/jira/browse/SPARK-40934
            feature_data = feature_data.set_index("date")
            feature_data.index = feature_data.index.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Convert to legacy AggregatePolygonResult-style construct:
            #       {datetime: [[float for each band] for each polygon]}
            # and wrap in UdfData/StructuredData
            timeseries = {
                date: [row.to_list()] for date, row in feature_data.iterrows()
            }
            structured_data = openeo.udf.StructuredData(
                description=f"Feature data {feature_index}",
                data=timeseries,
                type="dict",
            )
            udf_data = openeo.udf.UdfData(
                structured_data_list=[structured_data], user_context=context
            )
            # Apply UDF function
            processed = udf_function(udf_data)

            # Post-process UDF output
            if isinstance(processed, openeo.udf.UdfData):
                if len(processed.structured_data_list) != 1:
                    raise openeo.udf.OpenEoUdfException(
                        f"Expected single StructuredData result but got {len(processed.structured_data_list)}"
                    )
                processed = processed.structured_data_list[0].data
            if isinstance(processed, (int, float, str)):
                processed = pandas.Series([processed])
            elif isinstance(processed, dict):
                _log.warning(
                    f"Experimental UDF return data support: auto-convert dict to DataFrame "
                )
                processed = pandas.DataFrame.from_dict(processed, orient="index")
            elif isinstance(processed, list):
                _log.warning(
                    f"Experimental UDF return data support: auto-convert list to DataFrame "
                )
                processed = pandas.DataFrame(processed)

            if not isinstance(processed, (pandas.Series, pandas.DataFrame)):
                raise openeo.udf.OpenEoUdfException(
                    f"Failed to convert UDF return type to pandas Series/DataFrame: {type(processed)}"
                )
            return processed

        return callback
