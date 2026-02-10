def is_utm_epsg_code(epsg: int) -> bool:
    """Is given EPSG code a UTM CRS?"""
    return isinstance(epsg, int) and ((32601 <= epsg <= 32660) or (32701 <= epsg <= 32760))
