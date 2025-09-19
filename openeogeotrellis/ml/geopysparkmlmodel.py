from abc import ABC
from enum import Enum

from openeo_driver.datacube import DriverMlModel


class ModelArchitecture(Enum):
    RANDOM_FOREST = "random-forest"
    CATBOOST = "catboost"




class GeopysparkMlModel(DriverMlModel, ABC):

    @staticmethod
    def from_path(sc, path) -> "GeopysparkMlModel":
        """Create ML model instance from file path.

        :param sc: Spark context
        :param path: Path to model file or directory
        :return: Model instance
        """
        raise NotImplementedError

    def get_java_object(self):
        """Get the underlying Java object representation of the model.

        :return: Java object for the model
        """
        raise NotImplementedError
