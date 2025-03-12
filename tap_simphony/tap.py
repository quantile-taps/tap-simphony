"""Simphony tap class."""

from __future__ import annotations
from singer_sdk import Tap
from singer_sdk import typing as th
from tap_simphony import streams


class TapSimphony(Tap):
    """Simphony tap class."""

    name = "tap-simphony"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "location_reference",
            th.StringType,
            required=True,
        ),
        th.Property(
            "organization_identificer",
            th.StringType,
            required=True,
        ),
        th.Property(
            "auth_username",
            th.StringType,
            required=True,
        ),
        th.Property(
            "auth_password",
            th.StringType,
            required=True,
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.SimphonyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.MenuItemPrices(self),
            streams.MenuItemDimensions(self),
            streams.TaxDimensions(self),
            streams.RevenueCenterDimensions(self),
            streams.WorkstationsDimensions(self),
            streams.TenderMediaDimensions(self),
            streams.GuestChecks(self),
        ]


if __name__ == "__main__":
    TapSimphony.cli()
