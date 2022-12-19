from openeo.util import dict_no_none
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import FeatureUnsupportedException

OG_BATCH_RESULTS_BUCKET = "openeo-sentinelhub"


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
