"""Helpers for flow-level timeout parametrization."""

from __future__ import annotations

from functools import wraps
from inspect import Parameter, iscoroutinefunction, signature
from typing import Any, Callable, TypeVar

from prefect.utilities.timeout import timeout, timeout_async

F = TypeVar("F", bound=Callable[..., Any])

DEFAULT_TIMEOUT_SECONDS = 2 * 60 * 60


def with_flow_timeout_param(
    default_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> Callable[[F], F]:
    """Add a `timeout_seconds` parameter and enforce it at runtime.

    This decorator is intended to be used before `@flow`:

        @flow(...)
        @with_flow_timeout_param()
        def my_flow(...):
            ...
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
            async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                timeout_seconds = kwargs.pop("timeout_seconds", default_timeout_seconds)
                with timeout_async(seconds=timeout_seconds):
                    return await func(*args, **kwargs)

            async_wrapped.__signature__ = new_signature
            return async_wrapped  # type: ignore[return-value]

        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            timeout_seconds = kwargs.pop("timeout_seconds", default_timeout_seconds)
            with timeout(seconds=timeout_seconds):
                return func(*args, **kwargs)

        wrapped.__signature__ = new_signature
        return wrapped  # type: ignore[return-value]

    return decorator
