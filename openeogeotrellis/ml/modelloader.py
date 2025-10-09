import logging
import os
import shutil
import stat
import tempfile
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import geopyspark as gps
import requests

from openeo.util import deep_get
from openeo_driver.errors import OpenEOApiException, FileNotFoundException
from openeo_driver.utils import generate_unique_id

from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import s3_client, set_permissions
from openeogeotrellis.ml.geopysparkmlmodel import GeopysparkMlModel, ModelArchitecture
from openeogeotrellis.ml.geopysparkcatboostmodel import GeopySparkCatBoostModel
from openeogeotrellis.ml.geopysparkrandomforestmodel import GeopySparkRandomForestModel

logger = logging.getLogger(__name__)


class ModelLoader:
    """Handles loading of ML models from various sources"""

    @staticmethod
    def load_from_url(model_id: str, gps_batch_jobs: "GpsBatchJobs") -> GeopysparkMlModel:
        """Load ML model from HTTP URL using STAC metadata.

        Downloads model from URL specified in STAC metadata, supports both S3 and NFS storage.

        :param model_id: HTTP URL to STAC metadata describing the model
        :param gps_batch_jobs: Batch job manager for creating temporary directories
        :return: Loaded model instance
        """
        if not ModelLoader._is_valid_url(model_id):
            raise OpenEOApiException(message=f"Invalid URL format: {model_id}", status_code=400)

        try:
            metadata = ModelLoader._fetch_stac_metadata(model_id)
            architecture, model_url = ModelLoader._extract_model_info(metadata, model_id)
            return ModelLoader._load_model_by_architecture(architecture, model_url, gps_batch_jobs)
        except requests.RequestException as e:
            raise OpenEOApiException(message=f"Failed to fetch model metadata: {str(e)}", status_code=400) from e

    @staticmethod
    def load_from_batch_job(model_path: Path) -> GeopysparkMlModel:
        """Load ML model from batch job output directory.

        Attempts to load model from directory, falling back to packed .tar.gz format.
        NOTE: Currently only `GeopySparkRandomForestModel` is supported.

        :param model_path: Path to model directory or base path for packed model
        :return: Loaded model instance
        """
        if model_path.exists():
            logger.info(f"Loading ml_model using filename: {model_path}")
            return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{model_path}")

        packed_path = Path(f"{model_path}.tar.gz")
        if not packed_path.exists():
            raise FileNotFoundException(filename=str(model_path))

        shutil.unpack_archive(packed_path, extract_dir=model_path.parent, format="gztar")
        unpacked_path = str(packed_path).replace(".tar.gz", "")
        return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{unpacked_path}")

    @staticmethod
    def _fetch_stac_metadata(model_id: str) -> dict:
        with requests.get(model_id, timeout=60 * 60 * 6) as resp:
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

        checkpoints = ModelLoader._extract_checkpoints(metadata, model_id)
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
    def _load_model_by_architecture(
        architecture: ModelArchitecture, model_url: str, gps_batch_jobs: "GpsBatchJobs"
    ) -> GeopysparkMlModel:
        if architecture == ModelArchitecture.RANDOM_FOREST:
            use_s3 = ConfigParams().is_kube_deploy
            model_dir_path = ModelLoader._create_model_dir(gps_batch_jobs, use_s3)
            return ModelLoader._load_random_forest_model(model_url, model_dir_path, use_s3)
        elif architecture == ModelArchitecture.CATBOOST:
            return ModelLoader._load_catboost_model(model_url)

    @staticmethod
    def _load_random_forest_model(model_url: str, model_dir_path: str, use_s3: bool) -> GeopysparkMlModel:
        filename = "randomforest.model.tar.gz"
        if use_s3:
            return ModelLoader._load_random_forest_s3(model_url, model_dir_path, filename)
        return ModelLoader._load_random_forest_nfs(model_url, model_dir_path, filename)

    @staticmethod
    def _load_random_forest_s3(model_url: str, model_dir_path: str, filename: str) -> GeopysparkMlModel:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / filename

            # Download and extract
            ModelLoader._download_file(model_url, tmp_path)
            shutil.unpack_archive(tmp_path, extract_dir=tmp_dir, format="gztar")

            # Upload to S3
            unpacked_model_path = str(tmp_path).replace(".tar.gz", "")
            ModelLoader._upload_to_s3(unpacked_model_path, model_dir_path, tmp_dir)

            # Load model
            s3_path = f"s3a://{model_dir_path}/randomforest.model/"
            logger.info(f"Loading ml_model using filename: {s3_path}")
            return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=s3_path)

    @staticmethod
    def _create_model_dir(gps_batch_jobs: "GpsBatchJobs", use_s3: bool = False) -> str:
        """Create directory for temporary model storage.

        Creates either S3 path or NFS directory path for model storage.

        :param gps_batch_jobs: Batch job manager for accessing job output directories
        :param use_s3: Whether to use S3 storage (True) or NFS (False)
        :return: Path to created directory (S3 path or filesystem path)
        """
        if use_s3:
            # ML models will be loaded into the executors via the S3a filesystem connector.
            return f"openeo-ml-models-dev/{generate_unique_id(prefix='model')}"
        # ML models will be loaded into the executors via NFS (Network File System).
        # So we require a new directory that all executors from this sync/batch job can access.
        ml_models_dir = gps_batch_jobs.get_job_output_dir("ml_models")
        result_dir = ml_models_dir / generate_unique_id(prefix="model")
        ml_models_dir_exists = os.path.exists(ml_models_dir)

        logger.info(f"Creating directory: {result_dir}")
        os.makedirs(result_dir)

        if not ml_models_dir_exists:
            # Everyone can access the ml_models directory: `drwxrwxrwx user group`
            set_permissions(ml_models_dir, mode=stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        # Only the user has full access to this directory: `drwx------. user group`
        set_permissions(result_dir, mode=stat.S_IRWXU, user=None)
        return str(result_dir)

    @staticmethod
    def _load_random_forest_nfs(model_url: str, model_dir_path: str, filename: str) -> GeopysparkMlModel:
        dest_path = Path(model_dir_path) / filename

        # Download and extract to NFS directory
        ModelLoader._download_file(model_url, dest_path)
        shutil.unpack_archive(dest_path, extract_dir=model_dir_path, format="gztar")

        # The unpacked model path - this will be accessible via NFS to all executors
        unpacked_model_path = str(dest_path).replace(".tar.gz", "")
        logger.info(f"Loading ml_model using filename: {unpacked_model_path}")

        # Load the model using file:// protocol for NFS access
        return GeopySparkRandomForestModel.from_path(sc=gps.get_spark_context(), path=f"file:{unpacked_model_path}")

    @staticmethod
    def _load_catboost_model(model_url: str) -> GeopysparkMlModel:
        filename = "catboost_model.cbm.tar.gz"
        with tempfile.TemporaryDirectory() as tmp_dir:
            packed_path = Path(tmp_dir) / filename
            ModelLoader._download_file(model_url, packed_path)
            shutil.unpack_archive(packed_path, extract_dir=packed_path.parent, format="gztar")
            unpacked_path = str(packed_path).replace(".tar.gz", "")
            model_path = Path(unpacked_path) / "model"
            logger.info(f"Loading ml_model using file: {str(model_path)}")
            return GeopySparkCatBoostModel.from_path(str(model_path))

    @staticmethod
    def _download_file(url: str, dest_path: Path) -> None:
        logger.info(f"Downloading ml_model from {url} to {dest_path}")
        with requests.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

    @staticmethod
    def _upload_to_s3(local_path: str, s3_path: str, base_dir: str) -> None:
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

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
            return parsed.scheme in ["http", "https"] and parsed.netloc
        except Exception:
            return False
