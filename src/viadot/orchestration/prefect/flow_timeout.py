"""Utilities for adding runtime-configurable timeouts to Prefect flows.

The helper in this module lets a flow expose ``timeout_seconds`` as a regular
flow parameter while still enforcing the timeout through Prefect's timeout
context managers.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from inspect import Parameter, iscoroutinefunction, signature
from typing import TypeVar

from prefect.utilities.timeout import timeout, timeout_async


F = TypeVar("F", bound=Callable[..., object])

DEFAULT_TIMEOUT_SECONDS = 2 * 60 * 60


def with_flow_timeout_param(
    default_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> Callable[[F], F]:
    """Add a flow-level ``timeout_seconds`` parameter to a callable.

    The decorator updates the wrapped callable's signature so Prefect can expose
    ``timeout_seconds`` as a flow parameter, then executes the callable inside a
    timeout context. The injected parameter is keyword-only and defaults to
    ``default_timeout_seconds``.

    This decorator is intended to be applied below ``@flow`` so it runs first:

        @flow(...)
        @with_flow_timeout_param()
        def my_flow(...):
            ...

    If the wrapped callable already defines ``timeout_seconds``, the original
    callable is returned unchanged.

    Args:
        default_timeout_seconds: Default timeout applied when the caller does not
            provide ``timeout_seconds`` explicitly.

    Returns:
        A decorator that preserves the wrapped callable's metadata and signature
        while adding runtime timeout enforcement.
    """

    def decorator(func: F) -> F:
        func_signature = signature(func)
        if "timeout_seconds" in func_signature.parameters:
            return func

        timeout_param = Parameter(
            "timeout_seconds",
            kind=Parameter.KEYWORD_ONLY,
            default=default_timeout_seconds,
            annotation=int,
        )

        params = list(func_signature.parameters.values())
        kwargs_idx = next(
            (idx for idx, p in enumerate(params) if p.kind == Parameter.VAR_KEYWORD),
            None,
        )
        if kwargs_idx is None:
            new_params = [*params, timeout_param]
        else:
            new_params = [*params[:kwargs_idx], timeout_param, *params[kwargs_idx:]]

        new_signature = func_signature.replace(parameters=new_params)

        if iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapped(*args: object, **kwargs: object) -> object:
                timeout_seconds = kwargs.pop("timeout_seconds", default_timeout_seconds)
                with timeout_async(seconds=timeout_seconds):
                    return await func(*args, **kwargs)

            async_wrapped.__signature__ = new_signature
            return async_wrapped  # type: ignore[return-value]

        @wraps(func)
        def wrapped(*args: object, **kwargs: object) -> object:
            timeout_seconds = kwargs.pop("timeout_seconds", default_timeout_seconds)
            with timeout(seconds=timeout_seconds):
                return func(*args, **kwargs)

        wrapped.__signature__ = new_signature
        return wrapped  # type: ignore[return-value]

    return decorator
