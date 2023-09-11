from typing import Dict

import logging
from openeo.util import dict_no_none
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import FeatureUnsupportedException
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata

OG_BATCH_RESULTS_BUCKET = "openeo-sentinelhub"

logger = logging.getLogger(__name__)

def processing_options(collection_id: str, sar_backscatter_arguments: SarBackscatterArgs) -> dict:
    # TODO: split off validation so it can be used for CARD4L flow
    """As a side-effect, also validates the arguments."""

    if sar_backscatter_arguments.coefficient == "gamma0-terrain":
        backscatter_coefficient = "GAMMA0_TERRAIN"
    elif sar_backscatter_arguments.coefficient == "beta0":
        backscatter_coefficient = "BETA0"
    elif sar_backscatter_arguments.coefficient == "sigma0-ellipsoid":
        backscatter_coefficient = "SIGMA0_ELLIPSOID"
    elif sar_backscatter_arguments.coefficient == "gamma0-ellipsoid":
        backscatter_coefficient = "GAMMA0_ELLIPSOID"
    else:
        link = "https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#processing-options"
        raise FeatureUnsupportedException(
            "sar_backscatter: coefficient {c} is not supported for collection {coll}. {link}"
            .format(c=sar_backscatter_arguments.coefficient, coll=collection_id, link=link)
        )

    orthorectify = sar_backscatter_arguments.elevation_model != "off"

    if sar_backscatter_arguments.contributing_area:
        raise FeatureUnsupportedException("sar_backscatter: contributing_area is not supported")

    if sar_backscatter_arguments.ellipsoid_incidence_angle:
        raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle is not supported")

    if not sar_backscatter_arguments.noise_removal:
        raise FeatureUnsupportedException("sar_backscatter: noise_removal cannot be disabled")

    return dict_no_none(
        backCoeff=backscatter_coefficient,
        orthorectify=orthorectify,
        demInstance=sar_backscatter_arguments.elevation_model
    )


def assure_polarization_from_sentinel_bands(metadata: GeopysparkCubeMetadata, metadata_properties: Dict[str, object],
                                            job_id: str = None):
    """
    @param metadata:
    @param metadata_properties: Gets modified to have polarization filter when necessary
    @param job_id: (optional) job ID to log
    """
    log = logging.LoggerAdapter(logger, extra=dict_no_none(job_id=job_id))

    if metadata.auto_polarization() and "polarization" not in metadata_properties:
        bn = set(metadata.band_names)
        # https://docs.sentinel-hub.com/api/latest/data/sentinel-1-grd/#available-bands-and-data
        # Only run when relevant bands are present
        polarizations = []

        if "HH" in bn and "HV" in bn and "VV" not in bn and "VH" not in bn:
            polarizations = ["DH"]
        elif "VV" in bn and "VH" in bn and "HH" not in bn and "HV" not in bn:
            polarizations = ["DV"]
        elif "HH" in bn and "HV" not in bn and "VV" not in bn and "VH" not in bn:
            polarizations = ["HH", "DH", "SH"]
        elif "HV" in bn and "HH" not in bn and "VV" not in bn and "VH" not in bn:
            polarizations = ["HV", "DH"]
        elif "VV" in bn and "VH" not in bn and "HH" not in bn and "HV" not in bn:
            polarizations = ["VV", "DV", "SV"]
        elif "VH" in bn and "VV" not in bn and "HH" not in bn and "HV" not in bn:
            polarizations = ["VH", "DV"]

        if polarizations:
            log.info(f"No polarization was specified, using one based on band selection: {polarizations}")
            metadata_properties["polarization"] = {'in': polarizations}
        else:
            log.warning("No polarization was specified. This might give errors from Sentinelhub.")
