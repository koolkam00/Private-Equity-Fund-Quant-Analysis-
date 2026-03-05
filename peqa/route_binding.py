from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import click
from flask import current_app


@dataclass
class RouteRecord:
    rule: str
    endpoint: str
    view_func: Any
    options: dict[str, Any]


class _CliBinder:
    def __init__(self, owner: "AppBinder"):
        self.owner = owner

    def command(self, *args, **kwargs):
        def decorator(func):
            command = click.command(*args, **kwargs)(func)
            self.owner.cli_commands.append(command)
            return command

        return decorator


class AppBinder:
    """Record route-like registrations until a real Flask app exists."""

    def __init__(self):
        self.routes: list[RouteRecord] = []
        self.cli_commands: list[Any] = []
        self.context_processors: list[Any] = []
        self.cli = _CliBinder(self)

    def route(self, rule: str, **options):
        def decorator(func):
            endpoint = options.get("endpoint") or func.__name__
            self.routes.append(RouteRecord(rule=rule, endpoint=endpoint, view_func=func, options=dict(options)))
            return func

        return decorator

    def context_processor(self, func):
        self.context_processors.append(func)
        return func

    def register(self, flask_app, blueprints: dict[str, Any], route_groups: dict[str, str]):
        grouped = {name: [] for name in blueprints}
        for record in self.routes:
            group = route_groups.get(record.endpoint, "dashboard")
            grouped.setdefault(group, []).append(record)

            options = dict(record.options)
            options.pop("endpoint", None)
            flask_app.add_url_rule(record.rule, endpoint=record.endpoint, view_func=record.view_func, **options)

        for group_name, records in grouped.items():
            blueprint = blueprints[group_name]
            for record in records:
                options = dict(record.options)
                options.pop("endpoint", None)
                blueprint.add_url_rule(
                    record.rule,
                    endpoint=record.endpoint,
                    view_func=record.view_func,
                    **options,
                )
            flask_app.register_blueprint(blueprint)

        for command in self.cli_commands:
            flask_app.cli.add_command(command)
        for func in self.context_processors:
            flask_app.context_processor(func)

    def __getattr__(self, name: str):
        return getattr(current_app, name)

