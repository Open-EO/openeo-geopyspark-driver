import re
from enum import Enum

class ByteUnit(Enum):
    BYTE = 1
    KiB = 1 << 10
    MiB = 1 << 20
    GiB = 1 << 30
    TiB = 1 << 40
    PiB = 1 << 50

    def __init__(self, multiplier):
        self.multiplier = multiplier

    def convert_from(self, d, unit):
        return unit.convert_to(d, self)

    def convert_to(self, d, unit):
        if self.multiplier > unit.multiplier:
            ratio = self.multiplier // unit.multiplier
            if (2**63 - 1) // ratio < d:  # Simulating Long.MAX_VALUE
                raise ValueError(f"Conversion of {d} exceeds Long.MAX_VALUE in {self.name}. "
                                 f"Try a larger unit (e.g. MiB instead of KiB)")
            return d * ratio
        else:
            return d // (unit.multiplier // self.multiplier)

    def to_bytes(self, d):
        if d < 0:
            raise ValueError(f"Negative size value. Size must be positive: {d}")
        return d * self.multiplier

    def to_kib(self, d):
        return self.convert_to(d, ByteUnit.KiB)

    def to_mib(self, d):
        return self.convert_to(d, ByteUnit.MiB)

    def to_gib(self, d):
        return self.convert_to(d, ByteUnit.GiB)

    def to_tib(self, d):
        return self.convert_to(d, ByteUnit.TiB)

    def to_pib(self, d):
        return self.convert_to(d, ByteUnit.PiB)


BYTE_STRING_PATTERN = re.compile(r"([0-9]+)([a-z]+)?")
BYTE_STRING_FRACTION_PATTERN = re.compile(r"([0-9]+\.[0-9]+)([a-z]+)?")

BYTE_SUFFIXES = {
    "b": ByteUnit.BYTE,
    "k": ByteUnit.KiB,
    "m": ByteUnit.MiB,
    "g": ByteUnit.GiB,
    "t": ByteUnit.TiB,
    "p": ByteUnit.PiB
}

def byte_string_as(string, unit: ByteUnit = ByteUnit.BYTE) -> int:
    lower = string.lower().strip()

    try:
        m = BYTE_STRING_PATTERN.fullmatch(lower)
        fraction_matcher = BYTE_STRING_FRACTION_PATTERN.fullmatch(lower)

        if m:
            val = int(m.group(1))
            suffix = m.group(2)

            if suffix and suffix not in BYTE_SUFFIXES:
                raise ValueError(f"Invalid suffix: \"{suffix}\"")

            return unit.convert_from(val, BYTE_SUFFIXES[suffix] if suffix else unit)
        elif fraction_matcher:
            raise ValueError(f"Fractional values are not supported. Input was: {fraction_matcher.group(1)}")
        else:
            raise ValueError(f"Failed to parse byte string: {string}")

    except ValueError as e:
        byte_error = ("Size must be specified as bytes (b), kibibytes (k), mebibytes (m), "
                      "gibibytes (g), tebibytes (t), or pebibytes (p). E.g. 50b, 100k, or 250m.")
        raise ValueError(f"{byte_error}\n{e}")