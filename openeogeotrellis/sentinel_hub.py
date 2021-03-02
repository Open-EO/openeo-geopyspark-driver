from openeo.util import dict_no_none
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import FeatureUnsupportedException


def processing_options(sar_backscatter_arguments: SarBackscatterArgs) -> dict:
    """As a side-effect, also validates the arguments."""

    if sar_backscatter_arguments.coefficient in [None, "gamma0-terrain"]:
        backscatter_coefficient = "GAMMA0_TERRAIN"
    elif sar_backscatter_arguments.coefficient == "beta0":
        backscatter_coefficient = "BETA0"
    elif sar_backscatter_arguments.coefficient == "sigma0-ellipsoid":
        backscatter_coefficient = "SIGMA0_ELLIPSOID"
    elif sar_backscatter_arguments.coefficient == "gamma0-ellipsoid":
        backscatter_coefficient = "GAMMA0_ELLIPSOID"
    else:
        raise FeatureUnsupportedException("sar_backscatter: coefficient {c} is not supported"
                                          .format(c=sar_backscatter_arguments.coefficient))

    rtc = backscatter_coefficient == "GAMMA0_TERRAIN"
    orthorectify = rtc or sar_backscatter_arguments.local_incidence_angle

    # FIXME: support contributing_area because it is required by ard_normalized_radar_backscatter (under investigation
    #  by Anze)

    if sar_backscatter_arguments.ellipsoid_incidence_angle:
        raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle is not supported")

    if not sar_backscatter_arguments.noise_removal:
        raise FeatureUnsupportedException("sar_backscatter: noise_removal cannot be disabled")

    return dict_no_none(
        backCoeff=backscatter_coefficient,
        orthorectify=orthorectify,
        demInstance=sar_backscatter_arguments.elevation_model
    )
