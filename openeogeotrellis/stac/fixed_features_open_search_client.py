"""
Abstraction layer for OpenSearch client and feature builder.

Provides both JVM-based (production) and in-memory (testing) implementations.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional, Protocol

from py4j.java_gateway import JVMView


# ============================================================================
# Abstract interfaces (contracts)
# ============================================================================

class FeatureBuilder(Protocol):
    """Protocol defining the feature builder interface."""

    def withId(self, id: str) -> FeatureBuilder: ...
    def withNominalDate(self, date: str) -> FeatureBuilder: ...
    def withCRS(self, crs: str) -> FeatureBuilder: ...
    def withRasterExtent(self, minX: float, minY: float, maxX: float, maxY: float) -> FeatureBuilder: ...
    def withResolution(self, resolution: float) -> FeatureBuilder: ...
    def withBBox(self, minx: float, miny: float, maxx: float, maxy: float) -> FeatureBuilder: ...
    def withGeometryFromWkt(self, geometryWkt: str) -> FeatureBuilder: ...
    def withSelfUrl(self, url: str) -> FeatureBuilder: ...
    def addLink(self, href: str, title: str, pixel_value_offset: float, band_names: List[str]) -> FeatureBuilder: ...
    def build(self) -> Any: ...


class FixedFeaturesOpenSearchClient(ABC):
    """Abstract base class for FixedFeaturesOpenSearchClient."""

    @abstractmethod
    def feature_builder(self) -> FeatureBuilder:
        """Create a new feature builder instance."""
        pass

    @abstractmethod
    def add_feature(self, feature: Any) -> None:
        """Add a feature to the collection."""
        pass


class JvmFeatureBuilder:
    """Feature builder backed by JVM OpenSearchResponses.featureBuilder."""

    def __init__(self, jvm_builder):
        """
        Args:
            jvm_builder: JVM object from org.openeo.opensearch.OpenSearchResponses.featureBuilder()
        """
        self._builder = jvm_builder

    def withId(self, id: str) -> JvmFeatureBuilder:
        self._builder = self._builder.withId(id)
        return self

    def withNominalDate(self, date: str) -> JvmFeatureBuilder:
        self._builder = self._builder.withNominalDate(date)
        return self

    def withCRS(self, crs: str) -> JvmFeatureBuilder:
        self._builder = self._builder.withCRS(crs)
        return self

    def withRasterExtent(self, minX: float, minY: float, maxX: float, maxY: float) -> JvmFeatureBuilder:
        self._builder = self._builder.withRasterExtent(
            float(minX), float(minY), float(maxX), float(maxY)
        )
        return self

    def withResolution(self, resolution: float) -> JvmFeatureBuilder:
        self._builder = self._builder.withResolution(float(resolution))
        return self

    def withBBox(self, minx: float, miny: float, maxx: float, maxy: float) -> JvmFeatureBuilder:
        self._builder = self._builder.withBBox(
            float(minx), float(miny), float(maxx), float(maxy)
        )
        return self

    def withGeometryFromWkt(self, geometryWkt: str) -> JvmFeatureBuilder:
        self._builder = self._builder.withGeometryFromWkt(geometryWkt)
        return self

    def withSelfUrl(self, url: str) -> JvmFeatureBuilder:
        self._builder = self._builder.withSelfUrl(url)
        return self

    def addLink(self, href: str, title: str, pixel_value_offset: float, band_names: List[str]) -> JvmFeatureBuilder:
        self._builder = self._builder.addLink(href, title, float(pixel_value_offset), band_names)
        return self

    def build(self) -> Any:
        """Build and return the JVM feature object."""
        return self._builder.build()


class JvmFixedFeaturesOpenSearchClient(FixedFeaturesOpenSearchClient):
    """JVM-backed FixedFeaturesOpenSearchClient implementation."""

    def __init__(self, jvm: JVMView):
        self._jvm = jvm
        self._client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

    def feature_builder(self) -> JvmFeatureBuilder:
        """Create a new JVM-backed feature builder."""
        jvm_builder = self._jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
        return JvmFeatureBuilder(jvm_builder)

    def add_feature(self, feature: Any) -> None:
        """Add a feature to the JVM client."""
        self._client.addFeature(feature)

    def get_jvm_client(self):
        """Get the underlying JVM client object (for backwards compatibility)."""
        return self._client


# ============================================================================
# In-memory implementation (testing/debugging)
# ============================================================================

@dataclass
class InMemoryFeature:
    """In-memory representation of an OpenSearch feature (Python equivalent of JVM Feature)."""
    id: Optional[str] = None
    nominal_date: Optional[str] = None
    crs: Optional[str] = None
    raster_extent: Optional[tuple[float, float, float, float]] = None
    resolution: Optional[float] = None
    bbox: Optional[tuple[float, float, float, float]] = None
    geometry_wkt: Optional[str] = None
    self_url: Optional[str] = None
    links: List[dict] = field(default_factory=list)
    # Additional fields from Java Feature that could be added later:
    # tile_id, general_properties, deduplication_order_value, cloud_cover


class InMemoryFeatureBuilder:
    """Pure Python feature builder for testing without JVM."""

    def __init__(self, client: InMemoryFixedFeaturesOpenSearchClient):
        """
        Args:
            client: The parent client (used for registering the built feature)
        """
        self._client = client
        self._feature = InMemoryFeature()

    def withId(self, id: str) -> InMemoryFeatureBuilder:
        self._feature.id = id
        return self

    def withNominalDate(self, date: str) -> InMemoryFeatureBuilder:
        self._feature.nominal_date = date
        return self

    def withCRS(self, crs: str) -> InMemoryFeatureBuilder:
        self._feature.crs = crs
        return self

    def withRasterExtent(self, minX: float, minY: float, maxX: float, maxY: float) -> InMemoryFeatureBuilder:
        self._feature.raster_extent = (minX, minY, maxX, maxY)
        return self

    def withResolution(self, resolution: float) -> InMemoryFeatureBuilder:
        self._feature.resolution = resolution
        return self

    def withBBox(self, minx: float, miny: float, maxx: float, maxy: float) -> InMemoryFeatureBuilder:
        self._feature.bbox = (minx, miny, maxx, maxy)
        return self

    def withGeometryFromWkt(self, geometryWkt: str) -> InMemoryFeatureBuilder:
        self._feature.geometry_wkt = geometryWkt
        return self

    def withSelfUrl(self, url: str) -> InMemoryFeatureBuilder:
        self._feature.self_url = url
        return self

    def addLink(self, href: str, title: str, pixel_value_offset: float, band_names: List[str]) -> InMemoryFeatureBuilder:
        self._feature.links.append({
            "href": href,
            "title": title,
            "pixel_value_offset": pixel_value_offset,
            "band_names": band_names.copy(),
        })
        return self

    def build(self) -> InMemoryFeature:
        """Build and return the in-memory feature object."""
        return self._feature


class InMemoryFixedFeaturesOpenSearchClient(FixedFeaturesOpenSearchClient):
    """Pure Python FixedFeaturesOpenSearchClient for fast testing without JVM."""

    def __init__(self):
        self.features: List[InMemoryFeature] = []

    def feature_builder(self) -> InMemoryFeatureBuilder:
        """Create a new in-memory feature builder."""
        return InMemoryFeatureBuilder(self)

    def add_feature(self, feature: InMemoryFeature) -> None:
        """Add a feature to the in-memory collection."""
        self.features.append(feature)

    def get_features(self) -> List[InMemoryFeature]:
        """Get all collected features (for test assertions)."""
        return self.features

    def clear(self) -> None:
        """Clear all features (useful for test cleanup)."""
        self.features.clear()
