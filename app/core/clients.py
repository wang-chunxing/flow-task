import logging

from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.runtime import OpenTelemetryConfig, Runtime, TelemetryConfig
from tenacity import retry, stop_after_attempt, wait_exponential




logger = logging.getLogger(__name__)



class TemporalClientFactory:
    """Factory for creating Temporal Client instances"""

    @staticmethod
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    async def create(url: str) -> Client:
        return await Client.connect(url)