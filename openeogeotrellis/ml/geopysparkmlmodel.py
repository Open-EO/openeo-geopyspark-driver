from abc import ABC

from openeo_driver.datacube import DriverMlModel


class GeopysparkMlModel(DriverMlModel, ABC):

    @staticmethod
    def from_path(sc, path) -> "GeopysparkMlModel":
        raise NotImplementedError

    def get_java_object(self):
        raise NotImplementedError
