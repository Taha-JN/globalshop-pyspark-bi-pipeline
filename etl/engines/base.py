"""
etl/engines/base.py

Abstract Base Class defining the contract for GlobalShop ETL execution engines.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

from etl.schemas import StarSchemaContainer


class BaseETLEngine(ABC):
    """Abstract base class for data transformation engines (Spark, Pandas)."""

    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir

    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """Extract raw datasets from disk or object store."""
        pass

    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> StarSchemaContainer:
        """Apply data quality cleaning and build Kimball Star Schema entities."""
        pass

    @abstractmethod
    def load(self, star_schema: StarSchemaContainer) -> Dict[str, Any]:
        """Persist star schema entities to partitioned Parquet and record audit summary."""
        pass

    def run(self) -> Dict[str, Any]:
        """Orchestrate the end-to-end Extract -> Transform -> Load lifecycle."""
        raw_data = self.extract()
        star_schema = self.transform(raw_data)
        metrics = self.load(star_schema)
        return metrics
