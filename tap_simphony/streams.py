"""Stream type classes for tap-simphony."""

from __future__ import annotations
from singer_sdk import typing as th
from tap_simphony.client import SimphonyStream
from singer_sdk.helpers.types import Context
import requests
import pendulum
from singer_sdk import metrics
import typing as t


class MenuItemPrices(SimphonyStream):
    name = "menu_item_prices"
    path = "/getMenuItemPrices"
    primary_keys = ["num", "rvcNum"]
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
    primary_keys = ["num"]    
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
    primary_keys = ["num"]
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
    primary_keys = ["num"]
    replication_key = None

    records_jsonpath = "$.revenueCenters[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
    ).to_dict()


class WorkstationsDimensions(SimphonyStream):
    name = "workstations_dimensions"
    path = "/getLocationDimensions"
    primary_keys = ["wsNum"]
    replication_key = None

    records_jsonpath = "$.locations[0].workstations.[*]"

    schema = th.PropertiesList(
        th.Property("wsNum", th.IntegerType),
        th.Property("wsName", th.StringType),
    ).to_dict()


class TenderMediaDimensions(SimphonyStream):
    name = "tender_media_dimensions"
    path = "/getTenderMediaDimensions"
    primary_keys = ["num"]
    replication_key = None

    records_jsonpath = "$.tenderMedias[*]"

    schema = th.PropertiesList(
        th.Property("num", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("type", th.IntegerType),
    ).to_dict()


class GuestChecks(SimphonyStream):
    name = "guest_checks"
    path = "/getGuestChecks"
    primary_keys = ["guestCheckId"]
    replication_key = "opnLcl"

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
        payload = {
            "locRef": self.config["location_reference"],
        }

        # If making the first request, there is no `next_page_token` and we need to fetch the start date
        if not next_page_token:
            payload["busDt"] = self.starting_date.strftime("%Y-%m-%d")

        else:
            payload["busDt"] = next_page_token

        return payload
    
    @property
    def starting_date(self) -> pendulum.datetime:
        """
        Returns the starting date from when to start fetching time registrations as a pendulum object.
        """
        return pendulum.parse(self.get_starting_timestamp({}).strftime("%Y-%m-%d"))

    @property
    def period_to_retrieve(self) -> list[str]:
        """
        Because we have to create individual requests for each day, we need to generate a list of days. This method returns 
        a list of days between the start date and yesterday.
        """
        start_date = self.starting_date

        # We always want to retrieve the time registrations untill the previous day due to there might be a delay in the data
        end_date = pendulum.today() - pendulum.duration(days=1)

        # Get a list of dates between the start and end date. We use this for pagination.
        period = pendulum.interval(start_date, end_date)
        period_days = [day.strftime("%Y-%m-%d") for day in period.range('days')]

        return period_days

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Optional[str],
        ) -> t.Optional[str]:
        """This method returns the next date string untill we have reached the current day minus one day."""
        # If there is no `previous_token`, we have only requested the time registration for the starting date.
        # We set the `previous_token` to the starting date
        if not previous_token:
            previous_token = self.starting_date.strftime("%Y-%m-%d")

        # Add a day to the `previous_token` (i.e. last day for which time registrations have been requested)
        next_day_to_retrieve = pendulum.parse(previous_token).add(days=1)
        next_day_to_retrieve_formatted = next_day_to_retrieve.strftime("%Y-%m-%d")

        # If we passed the current date, we have requested all time registrations.
        # We end the request loop by returning None.
        if next_day_to_retrieve_formatted not in self.period_to_retrieve:
            return None

        return next_day_to_retrieve_formatted
    
    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """
        The original request_records method stops when no records are found in the last response.
        This is not the desired behavior for the guest_checks stream, because there can be days without guest checks.
        Therefore, we override the request_records method to keep requesting records until the end of the period_to_retrieve.

        We only commented out the exception handling in the original method.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                try:
                    first_record = next(records)

                except:
                    # If no records are found in the response, we prevent it from stopping
                    first_record = None
                    pass
                # except StopIteration:
                #     self.logger.info(
                #         "Pagination stopped after %d pages because no records were "
                #         "found in the last response",
                #         pages,
                #     )
                #     break
                yield first_record
                yield from records
                pages += 1

                paginator.advance(resp)