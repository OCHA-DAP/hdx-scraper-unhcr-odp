from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.unhcr_odp.pipeline import Pipeline

_HAS_STATS = ["AFG", "COD", "SSD", "SYR", "MLI", "BGD"]
_NO_STATS = ["SDN", "MMR", "TCD"]


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestUnhcr_odp",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                for country_config in configuration["countries"]:
                    iso3 = country_config["iso3"]
                    iso3_lower = iso3.lower()

                    stats_dataset = pipeline.generate_dataset(country_config)
                    odp_dataset = pipeline.generate_odp_dataset(country_config)

                    if iso3 in _HAS_STATS:
                        assert stats_dataset is not None
                        assert (
                            stats_dataset["name"]
                            == f"{iso3_lower}-forcibly-displaced-demographics"
                        )
                        stats_resources = {
                            r["name"] for r in stats_dataset.get_resources()
                        }
                        assert f"{iso3_lower}_unhcr_demographics.csv" in stats_resources
                        assert (
                            f"{iso3_lower}_unhcr_population_by_origin.csv"
                            in stats_resources
                        )
                    else:
                        assert stats_dataset is None

                    assert odp_dataset is not None
                    assert (
                        odp_dataset["name"]
                        == f"{iso3_lower}-forcibly-displaced-population-by-origin"
                    )
                    odp_resources = {r["name"] for r in odp_dataset.get_resources()}
                    assert (
                        f"{iso3_lower}_unhcr_odp_population_by_origin.csv"
                        in odp_resources
                    )

                    if iso3 == "COD":
                        # Regression: COD's odp_url used to point at the wrong
                        # population_group_id and returned zero refugees
                        resource = next(iter(odp_dataset.get_resources()))
                        assert "refugees" in resource["description"]
                        assert odp_dataset["methodology"] == "Registry"

                    if iso3 == "MMR":
                        # MMR's ODP data is 100% IDPs, not refugees. Title is
                        # generic (not type-specific); description carries the mix.
                        assert (
                            odp_dataset["title"]
                            == "Myanmar - Forcibly Displaced Population by Origin"
                        )
                        resource = next(iter(odp_dataset.get_resources()))
                        assert "IDP" in resource["description"]
                        assert odp_dataset["methodology"] == "Other"
                        assert odp_dataset["methodology_other"]

                    if iso3 == "AFG":
                        assert odp_dataset["methodology"] == "Registry"
                        assert "methodology_other" not in odp_dataset

                    if stats_dataset is not None:
                        # Demographics is a single-day headcount, not a year range
                        time_period = stats_dataset.get_time_period()
                        assert (
                            time_period["startdate"].date()
                            == time_period["enddate"].date()
                        )

                # Spot-check static YAML applies cleanly
                afg_dataset = pipeline.generate_dataset(configuration["countries"][0])
                afg_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
