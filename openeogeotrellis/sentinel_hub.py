from openeo.util import dict_no_none
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, FeatureUnsupportedException


def processing_options(sar_backscatter_arguments: SarBackscatterArgs) -> dict:
    """As a side-effect, also validates the arguments."""

    if sar_backscatter_arguments.elevation_model is not None and not sar_backscatter_arguments.orthorectify:
        raise OpenEOApiException(message="sar_backscatter: elevation_model is only used when orthorectify is enabled",
                                 status_code=400)

    if sar_backscatter_arguments.rtc:
        if not sar_backscatter_arguments.orthorectify:
            raise OpenEOApiException(message="sar_backscatter: orthorectify must be enabled for rtc", status_code=400)

        backscatter_coefficient = "GAMMA0_TERRAIN"
    else:
        backscatter_coefficient = "GAMMA0_ELLIPSOID"

    # FIXME: support contributing_area because it is required by ard_normalized_radar_backscatter (under investigation
    #  by Anze)

    if sar_backscatter_arguments.local_incidence_angle and not sar_backscatter_arguments.orthorectify:
        raise OpenEOApiException(message="sar_backscatter: orthorectify must be enabled for local_incidence_angle",
                                 status_code=400)

    if sar_backscatter_arguments.ellipsoid_incidence_angle:
        raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle is not supported")

    if not sar_backscatter_arguments.noise_removal:
        raise FeatureUnsupportedException("sar_backscatter: noise_removal cannot be disabled")

    return dict_no_none(
        backCoeff=backscatter_coefficient,
        orthorectify=sar_backscatter_arguments.orthorectify,
        demInstance=sar_backscatter_arguments.elevation_model
    )
