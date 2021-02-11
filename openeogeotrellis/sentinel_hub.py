from openeo.util import dict_no_none
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, FeatureUnsupportedException


def processing_options(sar_backscatter_arguments: SarBackscatterArgs) -> dict:
    if sar_backscatter_arguments.rtc:
        if not sar_backscatter_arguments.orthorectify:
            raise OpenEOApiException("sar_backscatter: rtc requires orthorectify")

        backscatter_coefficient = "GAMMA0_TERRAIN"
    else:
        raise FeatureUnsupportedException("sar_backscatter: only rtc is supported")

    if not sar_backscatter_arguments.noise_removal:
        raise FeatureUnsupportedException("sar_backscatter: only noise_removal is supported")

    # FIXME: support mask, contributing_area, local_incidence_angle and ellipsoid_incidence_angle
    return dict_no_none(
        backCoeff=backscatter_coefficient,
        orthorectify=sar_backscatter_arguments.orthorectify,
        demInstance=sar_backscatter_arguments.elevation_model
    )
