import logging
import os
import shutil
import stat
import tempfile
from abc import ABC
from enum import Enum
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import geopyspark as gps
import requests

from openeo.util import deep_get
from openeo_driver.datacube import DriverMlModel
from openeo_driver.errors import OpenEOApiException, InternalException
from openeo_driver.utils import generate_unique_id

from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import s3_client, add_permissions
from .geopysparkcatboostmodel import GeopySparkCatBoostModel
from .geopysparkrandomforestmodel import GeopySparkRandomForestModel
from ..backend import GpsBatchJobs

logger = logging.getLogger(__name__)


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


class MLModelLoader:
    """Handles loading of ML models from various sources"""

    @staticmethod
    def load_from_url(model_id: str, gps_batch_jobs: 'GpsBatchJobs') -> GeopysparkMlModel:
        """Load ML model from HTTP URL using STAC metadata.
        
        Downloads model from URL specified in STAC metadata, supports both S3 and NFS storage.
        
        :param model_id: HTTP URL to STAC metadata describing the model
        :param gps_batch_jobs: Batch job manager for creating temporary directories
        :return: Loaded model instance
        """
        if not MLModelLoader._is_valid_url(model_id):
            raise OpenEOApiException(message=f"Invalid URL format: {model_id}", status_code=400)

        try:
            metadata = MLModelLoader._fetch_stac_metadata(model_id)
            architecture, model_url = MLModelLoader._extract_model_info(metadata, model_id)

            use_s3 = ConfigParams().is_kube_deploy
            model_dir_path = MLModelLoader._create_model_dir(gps_batch_jobs, use_s3)
            
            return MLModelLoader._load_model_by_architecture(architecture, model_url, model_dir_path, use_s3)

        except requests.RequestException as e:
            raise OpenEOApiException(message=f"Failed to fetch model metadata: {str(e)}", status_code=400)
        except Exception as e:
            logger.error(f"Error loading model from URL {model_id}: {str(e)}")
            raise InternalException(f"Failed to load model: {str(e)}")

    @staticmethod
    def load_from_batch_job(model_path: Path) -> GeopysparkMlModel:
        """Load ML model from batch job output directory.
        
        Attempts to load model from directory, falling back to packed .tar.gz format.
        
        :param model_path: Path to model directory or base path for packed model
        :return: Loaded model instance
        """
        try:
            if model_path.exists():
                logger.info(f"Loading ml_model using filename: {model_path}")
                return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{model_path}")
            packed_path = Path(f"{model_path}.tar.gz")
            if packed_path.exists():
                return MLModelLoader._load_packed_model(packed_path, model_path.parent)
            raise OpenEOApiException(message=f"No random forest model found at {model_path}", status_code=400)

        except Exception as e:
            logger.error(f"Error loading model from path {model_path}: {str(e)}")
            raise InternalException(f"Failed to load model from path: {str(e)}")

    @staticmethod
    def _fetch_stac_metadata(model_id: str) -> dict:
        with requests.get(model_id, timeout=30) as resp:
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _extract_model_info(metadata: dict, model_id: str) -> Tuple[ModelArchitecture, str]:
        architecture_str = deep_get(metadata, "properties", "ml-model:architecture", default=None)
        if not architecture_str:
            raise OpenEOApiException(
                message=f"{model_id} does not specify a model architecture under properties.ml-model:architecture.",
                status_code=400,
            )

        try:
            architecture = ModelArchitecture(architecture_str)
        except ValueError:
            raise OpenEOApiException(message=f"Unsupported model architecture: {architecture_str}", status_code=400)

        checkpoints = MLModelLoader._extract_checkpoints(metadata, model_id)
        if len(checkpoints) > 1:
            raise OpenEOApiException(
                message=f"{model_id} contains multiple checkpoints which is not yet supported.", status_code=400
            )

        return architecture, checkpoints[0]["href"]

    @staticmethod
    def _extract_checkpoints(metadata: dict, model_id: str) -> List[dict]:
        assets = metadata.get("assets", {})
        checkpoints = []

        for asset_name, asset_data in assets.items():
            if "ml-model:checkpoint" in asset_data.get("roles", []):
                checkpoints.append(asset_data)

        if not checkpoints or not checkpoints[0].get("href"):
            raise OpenEOApiException(
                message=f"{model_id} does not contain a link to the ml model in its assets section.", status_code=400
            )

        return checkpoints

    @staticmethod
    def _load_model_by_architecture(architecture: ModelArchitecture, model_url: str, model_dir_path: str, use_s3: bool) -> GeopysparkMlModel:
        if architecture == ModelArchitecture.RANDOM_FOREST:
            return MLModelLoader._load_random_forest_model(model_url, model_dir_path, use_s3)
        elif architecture == ModelArchitecture.CATBOOST:
            return MLModelLoader._load_catboost_model(model_url, model_dir_path, use_s3)
        else:
            raise OpenEOApiException(
                message=f"Unsupported ml-model architecture: {architecture.value}", status_code=400
            )

    @staticmethod
    def _load_random_forest_model(model_url: str, model_dir_path: str, use_s3: bool) -> GeopysparkMlModel:
        filename = "randomforest.model.tar.gz"
        if use_s3:
            return MLModelLoader._load_random_forest_s3(model_url, model_dir_path, filename)
        return MLModelLoader._load_random_forest_nfs(model_url, model_dir_path, filename)

    @staticmethod
    def _load_random_forest_s3(model_url: str, model_dir_path: str, filename: str) -> GeopysparkMlModel:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / filename

            # Download and extract
            MLModelLoader._download_file(model_url, tmp_path)
            shutil.unpack_archive(tmp_path, extract_dir=tmp_dir, format="gztar")

            # Upload to S3
            unpacked_model_path = str(tmp_path).replace(".tar.gz", "")
            MLModelLoader._upload_to_s3(unpacked_model_path, model_dir_path, tmp_dir)

            # Load model
            s3_path = f"s3a://{model_dir_path}/randomforest.model/"
            logger.info(f"Loading ml_model using filename: {s3_path}")
            return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=s3_path)

    @staticmethod
    def _create_model_dir(gps_batch_jobs: 'GpsBatchJobs', use_s3: bool = False) -> str:
        """Create directory for temporary model storage.
        
        Creates either S3 path or NFS directory path for model storage.
        
        :param gps_batch_jobs: Batch job manager for accessing job output directories
        :param use_s3: Whether to use S3 storage (True) or NFS (False)
        :return: Path to created directory (S3 path or filesystem path)
        """
        if use_s3:
            # ML models will be loaded into the executors via the S3a filesystem connector.
            return f"openeo-ml-models-dev/{generate_unique_id(prefix='model')}"
        try:
            # ML models will be loaded into the executors via NFS (Network File System).
            # So we require a new directory that all executors from this sync/batch job can access.
            ml_models_dir = gps_batch_jobs.get_job_output_dir("ml_models")
            result_dir = ml_models_dir / generate_unique_id(prefix="model")
            ml_models_dir_exists = os.path.exists(ml_models_dir)

            logger.info(f"Creating directory: {result_dir}")
            os.makedirs(result_dir)

            if not ml_models_dir_exists:
                # Everyone can access the ml_models directory: `drwxrwxrwx user group`
                add_permissions(ml_models_dir, mode=stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            # Only the user has full access to this directory: `drwx------. user group`
            add_permissions(result_dir, mode=stat.S_IRWXU, user=None)
            return str(result_dir)

        except Exception as e:
            logger.error(f"Failed to create NFS model directory: {str(e)}")
            raise InternalException(f"NFS directory creation failed: {str(e)}")

    @staticmethod
    def _load_random_forest_nfs(model_url: str, model_dir_path: str, filename: str) -> GeopysparkMlModel:
        dest_path = Path(model_dir_path) / filename

        try:
            # Download and extract to NFS directory
            MLModelLoader._download_file(model_url, dest_path)
            shutil.unpack_archive(dest_path, extract_dir=model_dir_path, format="gztar")

            # The unpacked model path - this will be accessible via NFS to all executors
            unpacked_model_path = str(dest_path).replace(".tar.gz", "")
            logger.info(f"Loading ml_model using filename: {unpacked_model_path}")

            # Load the model using file:// protocol for NFS access
            return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{unpacked_model_path}")

        except Exception as e:
            logger.error(f"Failed to load Random Forest model to NFS: {str(e)}")
            raise InternalException(f"NFS model loading failed: {str(e)}")

    @staticmethod
    def _load_catboost_model(model_url: str, model_dir_path: str, use_s3: bool) -> GeopysparkMlModel:
        filename = "catboost_model.cbm"

        if use_s3:
            # TODO: Verify that local files work. If it does, we can remove the model_dir_path implementation.
            # Download the model to the tmp directory and load it as a java object.
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / filename
                MLModelLoader._download_file(model_url, tmp_path)
                return GeopySparkCatBoostModel.from_path(tmp_path)
        else:
            # Download to NFS directory for executor access
            file_path = Path(model_dir_path) / filename
            try:
                MLModelLoader._download_file(model_url, file_path)
                logger.info(f"Loading ml_model using filename: {file_path}")
                return GeopySparkCatBoostModel.from_path(str(file_path))
            except Exception as e:
                logger.error(f"Failed to load CatBoost model to NFS: {str(e)}")
                raise InternalException(f"NFS CatBoost loading failed: {str(e)}")

    @staticmethod
    def _download_file(url: str, dest_path: Path) -> None:
        try:
            logger.info(f"Downloading ml_model from {url} to {dest_path}")
            with requests.get(url, stream=True, timeout=300) as response:
                response.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            logger.error(f"Failed to download file from {url}: {str(e)}")
            raise InternalException(f"Download failed: {str(e)}")

    @staticmethod
    def _upload_to_s3(local_path: str, s3_path: str, base_dir: str) -> None:
        try:
            logger.info(f"Uploading ml_model to {s3_path}")
            path_split = s3_path.split("/", 1)
            if len(path_split) != 2:
                raise ValueError(f"Invalid S3 path format: {s3_path}")

            bucket, key = path_split[0], path_split[1]
            s3 = s3_client()

            for root, dirs, files in os.walk(local_path):
                for file in files:
                    local_file = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file, base_dir)
                    s3_key = f"{key}/{relative_path}"
                    s3.upload_file(local_file, bucket, s3_key)

        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise InternalException(f"S3 upload failed: {str(e)}")

    @staticmethod
    def _load_packed_model(packed_path: Path, directory: Path) -> GeopysparkMlModel:
        try:
            shutil.unpack_archive(packed_path, extract_dir=directory, format="gztar")
            unpacked_path = str(packed_path).replace(".tar.gz", "")
            return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{unpacked_path}")
        except Exception as e:
            logger.error(f"Failed to load packed model: {str(e)}")
            raise InternalException(f"Packed model loading failed: {str(e)}")

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
            return parsed.scheme in ["http", "https"] and parsed.netloc
        except Exception:
            return False
