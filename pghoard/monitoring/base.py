# Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
from typing import Any, Dict, Optional


class MetricsClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def gauge(self, metric: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    def increase(self, metric: str, inc_value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        pass

    def unexpected_exception(self, ex: Exception, where: str, tags: Optional[Dict[str, str]] = None) -> None:
        pass
