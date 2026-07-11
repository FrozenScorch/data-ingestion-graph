"""Stable exception hierarchy exposed by the SDK."""


class IngestionError(Exception):
    """Base class for SDK errors."""


class ConfigurationError(IngestionError):
    """Connector or runtime configuration is invalid."""


class AuthenticationError(IngestionError):
    """A connector could not authenticate."""


class PermissionDeniedError(IngestionError):
    """A connector is authenticated but lacks required access."""


class RateLimitError(IngestionError):
    """A remote rate limit could not be satisfied within the retry policy."""


class ProtocolError(IngestionError):
    """A source or destination violated the ingestion protocol."""


class PluginError(IngestionError):
    """A plugin could not be discovered or loaded."""
