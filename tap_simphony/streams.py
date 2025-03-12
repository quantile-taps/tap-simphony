"""Stream type classes for tap-simphony."""

from __future__ import annotations
import typing as t
from singer_sdk import typing as th
from tap_simphony.client import SimphonyStream
from singer_sdk.helpers.types import Context
from datetime import datetime, timedelta

class MenuItemPrices(SimphonyStream):
    name = "menu_item_prices"
    path = "/getMenuItemPrices"
    replication_key = None

    records_jsonpath = "$.menuItemPrices[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("rvcNum", th.IntegerType),
        th.Property("prcLvlNum", th.IntegerType),
        th.Property("prcLvlName", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("effFrDt", th.DateTimeType),
        th.Property("effToDt", th.DateTimeType),
    ).to_dict()


class MenuItemDimensions(SimphonyStream):
    name = "menu_item_dimensions"
    path = "/getMenuItemDimensions"
    replication_key = None

    records_jsonpath = "$.menuItems[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("name2", th.StringType),
        th.Property("majGrpNum", th.IntegerType),
        th.Property("majGrpName", th.StringType),
        th.Property("famGrpNum", th.IntegerType),
        th.Property("famGrpName", th.StringType),
    ).to_dict()


class TaxDimensions(SimphonyStream):
    name = "tax_dimensions"
    path = "/getTaxDimensions"
    replication_key = None

    records_jsonpath = "$.taxes[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("type", th.IntegerType),
        th.Property("taxRate", th.IntegerType),
    ).to_dict()


class RevenueCenterDimensions(SimphonyStream):
    name = "revenue_center_dimensions"
    path = "/getRevenueCenterDimensions"
    replication_key = None

    records_jsonpath = "$.revenueCenters[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
    ).to_dict()


class WorkstationsDimensions(SimphonyStream):
    name = "workstations_dimensions"
    path = "/getLocationDimensions"
    replication_key = None

    records_jsonpath = "$.locations[0].workstations.[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
    ).to_dict()


class GuestChecks(SimphonyStream):
    name = "guest_checks"
    path = "/getGuestChecks"
    primary_keys = ["guestCheckId"]
    replication_key = None

    records_jsonpath = "$.guestChecks[*]"

    schema = th.PropertiesList(
        th.Property("guestCheckId", th.IntegerType),
        th.Property("chkNum", th.IntegerType),
        th.Property("opnBusDt", th.DateType),
        th.Property("opnUTC", th.DateTimeType),
        th.Property("opnLcl", th.DateTimeType),
        th.Property("clsdBusDt", th.DateType),
        th.Property("clsdUTC", th.DateTimeType),
        th.Property("clsdLcl", th.DateTimeType),
        th.Property("lastTransUTC", th.DateTimeType),
        th.Property("lastTransLcl", th.DateTimeType),
        th.Property("lastUpdatedUTC", th.DateTimeType),
        th.Property("lastUpdatedLcl", th.DateTimeType),
        th.Property("clsdFlag", th.BooleanType),
        th.Property("cancelFlag", th.BooleanType),
        th.Property("subTtl", th.NumberType),
        th.Property("nonTxblSlsTtl", th.StringType),
        th.Property("chkTtl", th.NumberType),
        th.Property("dscTtl", th.NumberType),
        th.Property("payTtl", th.NumberType),
        th.Property("balDueTtl", th.StringType),
        th.Property("rvcNum", th.IntegerType),
        th.Property("otNum", th.IntegerType),
        th.Property("ocNum", th.IntegerType),
        th.Property("empNum", th.IntegerType),
        th.Property("numSrvcRd", th.IntegerType),
        th.Property("errorCorrectTtl", th.NumberType),
        th.Property(
            "taxes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("taxNum", th.IntegerType),
                    th.Property("txblSlsTtl", th.NumberType),
                    th.Property("taxCollTtl", th.NumberType),
                    th.Property("taxRate", th.NumberType),
                    th.Property("type", th.IntegerType),
                )
            )
        ),
        th.Property(
            "detailLines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("guestCheckLineItemId", th.IntegerType),
                    th.Property("rvcNum", th.IntegerType),
                    th.Property("dtlOtNum", th.IntegerType),
                    th.Property("dtlOcNum", th.IntegerType),
                    th.Property("lineNum", th.IntegerType),
                    th.Property("dtlId", th.IntegerType),
                    th.Property("parDtlId", th.IntegerType),
                    th.Property("detailUTC", th.DateTimeType),
                    th.Property("detailLcl", th.DateTimeType),
                    th.Property("lastUpdateUTC", th.DateTimeType),
                    th.Property("lastUpdateLcl", th.DateTimeType),
                    th.Property("busDt", th.DateType),
                    th.Property("wsNum", th.IntegerType),
                    th.Property("refInfo1", th.StringType),
                    th.Property("dspTtl", th.NumberType),
                    th.Property("dspQty", th.IntegerType),
                    th.Property("aggTtl", th.NumberType),
                    th.Property("aggQty", th.IntegerType),
                    th.Property("chkEmpId", th.IntegerType),
                    th.Property("chkEmpNum", th.IntegerType),
                    th.Property("svcRndNum", th.IntegerType),
                    th.Property("numerator", th.IntegerType),
                    th.Property("denominator", th.IntegerType),
                    th.Property(
                        "tenderMedia",
                        th.ObjectType(
                            th.Property("tmedNum", th.IntegerType),
                        )
                    ),
                    th.Property(
                        "menuItem",
                        th.ObjectType(
                            th.Property("miNum", th.IntegerType),
                            th.Property("modFlag", th.BooleanType),
                            th.Property("inclTax", th.NumberType),
                            th.Property("activeTaxes", th.StringType),
                            th.Property("prcLvl", th.IntegerType),
                        )
                    ),
                    th.Property(
                        "other",
                        th.ObjectType(
                            th.Property("detailType", th.IntegerType),
                            th.Property("detailNum", th.IntegerType),
                        )
                    ),
                )
            )
        ),
    ).to_dict()

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        return {
            "locRef": self.config["location_reference"],

            # TODO: add state logic to loop over the dates. By e.g. doing a sliding window.
            "busDt": "2025-03-05",
            # "opnBusDt": "2025-03-05",
            # "clsdBusDt": "2025-03-06",
        }
