#!/usr/bin/python
"""Unhcr_odp scraper"""

import logging

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

# pop_type_name -> phrase used in resource descriptions ("including {phrase}")
_POP_TYPE_DESC_LABELS = {
    "Refugees": "refugees",
    "IDPs": "internally displaced persons (IDPs)",
    "Asylum-Seekers": "asylum-seekers",
    "Asylum-seekers": "asylum-seekers",
    "Stateless": "stateless persons",
    "Returnees": "returnees",
    "Others of Concern": "others of concern",
    "Host Community": "host community",
}


def _join_oxford(items: list) -> str:
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def _make_dataset(
        self,
        iso3: str,
        country_name: str,
        slug: str,
        title: str,
        start_date,
        end_date=None,
        methodology: str = "Registry",
        methodology_other: str | None = None,
    ) -> Dataset | None:
        dataset = Dataset({"name": slug, "title": title})
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(self._configuration["tags"])
        dataset["methodology"] = methodology
        if methodology_other:
            dataset["methodology_other"] = methodology_other
        try:
            dataset.add_country_location(iso3)
        except HDXError:
            logger.error(f"Couldn't find country {iso3}, skipping")
            return None
        return dataset

    def generate_dataset(self, country_config: dict) -> Dataset | None:
        iso3 = country_config["iso3"]
        iso3_lower = iso3.lower()
        country_name = Country.get_country_name_from_iso3(iso3)

        # Fetch demographics (no year param — API returns latest published year)
        demographics_url = self._configuration["demographics_url"].format(iso3=iso3)
        demographics_data = self._retriever.download_json(
            demographics_url, f"demographics_{iso3_lower}.json"
        )
        demographics_items = demographics_data.get("items", [])
        if not demographics_items:
            logger.warning(f"No demographics data for {iso3}, skipping")
            return None

        year = demographics_items[0]["year"]

        population_url = self._configuration["population_url"].format(
            iso3=iso3, year=year
        )
        population_data = self._retriever.download_json(
            population_url, f"population_{iso3_lower}.json"
        )
        population_items = population_data.get("items", [])

        dataset = self._make_dataset(
            iso3,
            country_name,
            slug=f"{iso3_lower}-forcibly-displaced-demographics",
            title=f"{country_name} - Forcibly Displaced Demographics and Population by Origin",
            start_date=parse_date(f"{year}-12-31"),
        )
        if not dataset:
            return None
        dataset["dataset_source"] = "UNHCR Refugee Statistics API"
        dataset["notes"] = (
            "This dataset contains age and gender disaggregated demographics and population "
            "totals for forcibly displaced persons, including refugees, asylum-seekers, and "
            "internally displaced persons. Data is sourced from the "
            "[UNHCR Refugee Statistics API](https://api.unhcr.org)."
        )

        filename = f"{iso3_lower}_unhcr_demographics.csv"
        dataset.generate_resource(
            folder=self._tempdir,
            filename=filename,
            rows=demographics_items,
            resourcedata={
                "name": filename,
                "description": (
                    f"Forcibly displaced and stateless persons in {country_name} disaggregated "
                    f"by age group and sex, including refugees, asylum-seekers, internally displaced "
                    f"persons (IDPs), and stateless persons."
                ),
            },
            headers=list(demographics_items[0].keys()),
            encoding="utf-8-sig",
        )

        if population_items:
            filename = f"{iso3_lower}_unhcr_population_by_origin.csv"
            dataset.generate_resource(
                folder=self._tempdir,
                filename=filename,
                rows=population_items,
                resourcedata={
                    "name": filename,
                    "description": (
                        f"Forcibly displaced and stateless persons hosted in {country_name} by country of origin, "
                        f"including refugees, asylum-seekers, internally displaced persons (IDPs), "
                        f"returned refugees, stateless persons, and host communities."
                    ),
                },
                headers=list(population_items[0].keys()),
                encoding="utf-8-sig",
            )

        return dataset

    def generate_odp_dataset(self, country_config: dict) -> Dataset | None:
        iso3 = country_config["iso3"]
        iso3_lower = iso3.lower()
        country_name = Country.get_country_name_from_iso3(iso3)

        odp_url = country_config.get("odp_url")
        if not odp_url:
            return None

        odp_data = self._retriever.download_json(odp_url, f"odp_{iso3_lower}.json")
        odp_items = odp_data.get("data", [])
        if not odp_items:
            logger.warning(f"No ODP data for {iso3}, skipping")
            return None

        pop_types = list(
            dict.fromkeys(
                item["pop_type_name"] for item in odp_items if item.get("pop_type_name")
            )
        )
        desc_labels = [_POP_TYPE_DESC_LABELS.get(pt, pt.lower()) for pt in pop_types]
        description_phrase = (
            _join_oxford(desc_labels) if desc_labels else "forcibly displaced persons"
        )

        title = f"{country_name} - Forcibly Displaced Population by Origin"

        data_types = {
            item["pop_data_type_name"]
            for item in odp_items
            if item.get("pop_data_type_name")
        }
        if data_types == {"Estimated"}:
            methodology = "Other"
            methodology_other = "Estimated"
        else:
            methodology = "Registry"
            methodology_other = None

        dataset = self._make_dataset(
            iso3,
            country_name,
            slug=f"{iso3_lower}-forcibly-displaced-population-by-origin",
            title=title,
            start_date=parse_date(odp_items[0]["date"]),
            methodology=methodology,
            methodology_other=methodology_other,
        )
        if not dataset:
            return None
        dataset["dataset_source"] = "UNHCR Operational Data Portal (ODP)"
        dataset["notes"] = (
            "This dataset contains operational population figures by country of origin "
            "for forcibly displaced persons. Data is sourced from the "
            "[UNHCR Operational Data Portal](https://data.unhcr.org)."
        )

        filename = f"{iso3_lower}_unhcr_odp_population_by_origin.csv"
        dataset.generate_resource(
            folder=self._tempdir,
            filename=filename,
            rows=odp_items,
            resourcedata={
                "name": filename,
                "description": (
                    f"Operational population figures for {country_name} disaggregated by country of origin, "
                    f"including {description_phrase}."
                ),
            },
            headers=list(odp_items[0].keys()),
            encoding="utf-8-sig",
        )

        return dataset
