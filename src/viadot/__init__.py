"""The viadot library."""

import logging


# Remove trash Azure INFO logs which contain low-level debugging information
# but keep WARNING and higher ones in case something actually important happens.

azure_clutter_logger_1 = logging.getLogger(
    "azure.core.pipeline.policies.http_logging_policy",
)
azure_clutter_logger_1.setLevel(logging.WARNING)


azure_clutter_logger_2 = logging.getLogger(
    "azure.identity.aio._internal.get_token_mixin",
)
azure_clutter_logger_2.setLevel(logging.WARNING)
