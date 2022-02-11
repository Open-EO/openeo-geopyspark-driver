import pathlib
from typing import Dict
from openeo_driver.save_result import SaveResult
import geopyspark as gps
from pyspark.mllib.util import JavaSaveable


class MLModel(SaveResult):
    def __init__(self, model: JavaSaveable):
        super().__init__(format='standard', options={})
        self._model = model

    def write_assets(self, directory: str) -> Dict:
        """
        Save generated assets into a directory, return asset metadata.

        :return: STAC assets dictionary: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#assets
        """
        directory = pathlib.Path(directory).parent
        filename = str(pathlib.Path(directory) / "mlmodel.model")
        self._model.save(gps.get_spark_context() , filename)
        return {filename:{"href":filename}}

    def save_ml_model(self, directory: str) -> Dict:
        return self.write_assets(directory)