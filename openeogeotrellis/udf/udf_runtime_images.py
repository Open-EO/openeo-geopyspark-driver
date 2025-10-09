import dataclasses
import itertools
import re
import typing
from typing import List, Dict, Optional, Iterable, Tuple, Set
import logging
from openeogeotrellis.config import GpsBackendConfig, get_backend_config
from openeogeotrellis.udf import UdfRuntimeSpecified


_log = logging.getLogger(__name__)


class _UdfRuntimeAndVersion(typing.NamedTuple):
    """
    UDF runtime name+version pair (both required)
    """
    name: str
    version: str
    # Score (higher is better) for determining the default version
    # of a UDF runtime (in `GET /udf_runtimes` response).
    preference: int = 0


@dataclasses.dataclass(frozen=True)
class _ImageData:
    """
    Internal record with data about a (docker) container image
    and associated UDF runtime information
    """

    # Full container image reference (typically including registry and tag)
    image_ref: str

    # List of aliases for users to use as "image-name" job option (`JobOptions.image_name`)
    # Note that is this less standardized compared to UDF runtime version
    # TODO: opportunity to minimize/eliminate this field? E.g. derive it from udf_runtime_versions?
    # (e.g. ["python311"])
    image_aliases: List[str] = dataclasses.field(default_factory=list)

    # Score (higher is better) for determining the default image
    # and to use as tie breaker when multiple images match a request.
    preference: int = 0

    # UDF runtimes supported by this image
    # Used (after filtering and massaging) to expose to end user in ``ET /udf_runtimes` response
    udf_runtimes: List[_UdfRuntimeAndVersion] = dataclasses.field(default_factory=list)

    # Mapping of UDF runtime library names to versions included in this image
    # (e.g. {"numpy": "1.23.5", "pandas": "1.5.3"})
    udf_runtime_libraries: Dict[str, str] = dataclasses.field(default_factory=dict)

    def matches_udf_runtime(self, runtime: UdfRuntimeSpecified) -> bool:
        """Whether this image supports the given UDF runtime (name+optional version)"""
        return any(
            runtime.name == r.name and (runtime.version is None or runtime.version == r.version)
            for r in self.udf_runtimes
        )


class UdfRuntimeImageRepository:
    """
    Helper/Adapter between
    - backend configuration
    - version handling of the "Python UDF runtime" (per standard openEO API, e.g. `GET /udf_runtimes`)
    - batch job container images and how to resolve aliases or UDF runtime info to images
    """


    def __init__(self, images: List[_ImageData]):
        self._images: List[_ImageData] = images

    @classmethod
    def from_config(cls, config: Optional[GpsBackendConfig] = None):
        config = config or get_backend_config()

        if batch_runtime_to_image := config.batch_runtime_to_image:
            return cls._from_config_batch_runtime_to_image(batch_runtime_to_image=batch_runtime_to_image)
        else:
            # TODO: support a new, more flexible config format?
            return cls(images=[])

    @classmethod
    def _from_config_batch_runtime_to_image(cls, batch_runtime_to_image: dict):
        """Ad-hoc adapter for legacy `batch_runtime_to_image` config format"""
        # TODO get rid of `batch_runtime_to_image` in longer term?
        image_entries = []
        # TODO: toggle the Jep variant through config too?
        runtime_names = ["Python", "Python-Jep"]
        for alias, image_ref in batch_runtime_to_image.items():
            # Ad-hoc conversion of current "python311" alias to "3.11" UDF runtime version
            match = re.match(r"^py(thon)?(?P<major>\d)\.?(?P<minor>\d{1,2})$", alias, flags=re.I)
            if match:
                udf_runtimes = [
                    _UdfRuntimeAndVersion(
                        name=n,
                        version=v,
                        # Prefer highest, but least specific major version
                        preference=int(match.group("major")) * 10 - len(v),
                    )
                    for n in runtime_names
                    for v in [f"{match.group('major')}.{match.group('minor')}", f"{match.group('major')}"]
                ]
                image_preference = int(match.group("major")) * 100 + int(match.group("minor"))
            else:
                _log.warning(f"Failed to guess python version from image alias {alias!r}.")
                udf_runtimes = [_UdfRuntimeAndVersion(name=n, version=alias) for n in runtime_names]
                image_preference = 0

            image_entries.append(
                _ImageData(
                    image_ref=image_ref,
                    image_aliases=[alias],
                    preference=image_preference,
                    udf_runtimes=udf_runtimes,
                    # TODO: how to specify/detect library versions?
                    udf_runtime_libraries={},
                )
            )
        return cls(images=image_entries)

    def get_udf_runtimes_response(self) -> dict:
        """Generate `GET /udf_runtimes` response"""
        # First collect defaults per runtime name (group by name, pick version with highest default_priority)
        defaults: Dict[str, str] = {
            name: max(group, key=lambda x: x[2])[1]
            for name, group in itertools.groupby(
                sorted((rt.name, rt.version, rt.preference) for im in self._images for rt in im.udf_runtimes),
                key=lambda x: x[0],
            )
        }

        # Initial response
        response = {
            name: {
                # TODO: functionality to provide better title and description
                "title": f"{name}",
                "type": "language",
                "default": default,
                "versions": {},
            }
            for name, default in defaults.items()
        }

        # Add versions and libraries
        for image_data in self._images:
            for runtime in image_data.udf_runtimes:
                # TODO: also support fields "deprecated" and "experimental"?
                libraries = {k: {"version": v} for k, v in image_data.udf_runtime_libraries.items()}
                if runtime.version in response[runtime.name]["versions"]:
                    _log.warning(f"Duplicate {runtime=}: merging library info")
                    response[runtime.name]["versions"][runtime.version]["libraries"] = self._merge_libraries(
                        response[runtime.name]["versions"][runtime.version]["libraries"],
                        libraries,
                    )
                else:
                    response[runtime.name]["versions"][runtime.version] = {"libraries": libraries}

        return response

    @staticmethod
    def _merge_libraries(libs1: dict, libs2: dict) -> dict:
        """Merge two library listings, only keeping info if identical."""
        merged = {}
        for name in set(libs1).union(libs2):
            if libs1.get(name) == libs2.get(name):
                merged[name] = libs1.get(name)
            else:
                v1 = libs1.get(name, {}).get("version", "n/a")
                v2 = libs2.get(name, {}).get("version", "n/a")
                merged[name] = {"version": f"{v1}|{v2}"}
        return merged

    def get_default_image(self) -> str:
        best: _ImageData = max(self._images, key=lambda x: x.preference)
        return best.image_ref

    def get_image_from_udf_runtimes(self, runtimes: Iterable[UdfRuntimeSpecified]) -> str:
        """
        Try to resolve the set of UDF runtimes to a fitting container image reference

        :param runtimes: Iterable of UDF runtimes specified by user
        :return: container image reference
        """
        runtimes = set(runtimes)

        # Score each image by how many of the requested runtimes it supports
        scored = [
            (
                img,
                sum(img.matches_udf_runtime(r) for r in runtimes),
                # Tie breaker
                img.preference,
            )
            for img in self._images
        ]
        best_image, best_score, _ = max(scored, key=lambda s: (s[1], s[2]))

        if best_score != len(runtimes):
            _log.warning(
                f"No image matches all {runtimes=}, best match {best_score}/{len(runtimes)}: {best_image.image_ref!r}"
            )
        else:
            _log.info(f"Best image match for {runtimes=}: {best_image.image_ref!r}")
        return best_image.image_ref
