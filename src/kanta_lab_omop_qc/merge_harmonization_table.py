from argparse import ArgumentParser
from pathlib import Path

import polars as pl


DESCRIPTION = (
    "I take the Kanta lab harmonization table from Google Sheet as a CSV, and "
    "add new columns to it."
)


def main():
    args = cli_init()

    args.func(**vars(args))


def cli_init():
    parser = ArgumentParser(description=DESCRIPTION)

    subparsers = parser.add_subparsers()

    parser.add_argument(
        "--harmonization-table",
        help="path to the harmonization table (CSV)",
        type=Path,
        required=True
    )

    parser.add_argument(
        "--output",
        help="path to output file of merged table (CSV)",
        type=Path,
        required=True
    )

    parser_top10_lab_values = subparsers.add_parser(
        "add_top10_lab_values",
        help="Add the top 10 lab values for each row."
    )
    parser_top10_lab_values.add_argument(
        "--finregistry-stats",
        help="path to FinRegistry stats on Kanta lab values (CSV)",
        type=Path,
        required=True
    )
    parser_top10_lab_values.set_defaults(func=add_top10_lab_values)

    parser_keep_90percent = subparsers.add_parser(
        "keep_90percent",
        help="Filter out rows that don't make 90%% of the data for each OMOP ID"
    )
    parser_keep_90percent.set_defaults(func=filter_90percent)

    args = parser.parse_args()

    return args


def add_top10_lab_values(*, harmonization_table, finregistry_stats, output, **_kwargs):
    dataf_table = pl.read_csv(
        harmonization_table,
        dtypes={
            "OMOP Concept ID": pl.String,
            "OMOP Concept name": pl.String,
            "Lab test ID": pl.String,
            "Lab test abbreviation": pl.String,
            "Lab test unit": pl.String,
            "N people": pl.String,
            "N Records": pl.String,
            "URL": pl.String,
            "Link to OHDSI Athena": pl.String,
            "New mapping": pl.String,
            "Comment": pl.String,
        }
    )

    dataf_stats = (
        pl.read_csv(
            finregistry_stats,
            dtypes={
                "OMOP_ID": pl.String,
                "LAB_ID": pl.String,
                "LAB_ABBREVIATION": pl.String,
                "LAB_UNIT": pl.String,
                "LAB_VALUE": pl.String,
                "NPeople": pl.UInt64,
                "NRecords": pl.UInt64,
            }
        )
        .sort(
            by=["OMOP_ID", "LAB_ID", "LAB_ABBREVIATION", "LAB_UNIT", "NPeople", "NRecords", "LAB_VALUE"],
            descending=True
        )
        .group_by(["OMOP_ID", "LAB_ID", "LAB_ABBREVIATION", "LAB_UNIT"])
        .agg(
            (
                pl.col("LAB_VALUE")
                .head(10)
                .alias("Top10LabValues")
            )
        )
        .with_columns(
            (
                pl.col("Top10LabValues")
                .list.join(separator=" ; ")
            )
        )
    )

    (
        dataf_table
        .join(
            other=dataf_stats,
            how="left",
            left_on=[
                "OMOP Concept ID",
                "Lab test ID",
                "Lab test abbreviation",
                "Lab test unit"
            ],
            right_on=[
                "OMOP_ID",
                "LAB_ID",
                "LAB_ABBREVIATION",
                "LAB_UNIT"
            ]
        )
        .write_csv(output)
    )


def filter_90percent(*, harmonization_table, output, **_kwargs):
    (
        pl.scan_csv(
            harmonization_table,
            dtypes={
                "OMOP Concept ID": pl.String,
                "N Records": pl.UInt64
            }
        )
        .with_columns(
            # Add row identifier so we can recover the original sorting later on.
            pl.col("OMOP Concept ID").cum_count().alias("rowid")
        )
        .with_columns(
            # Further stats depends on each OMOP group being sorted max to min.
            pl.col("N Records").sort(descending=True).over("OMOP Concept ID")
        )
        .with_columns(
            pl.col("N Records").sum().over("OMOP Concept ID").alias("Total_per_omopid"),
            pl.col("N Records").cum_sum().over("OMOP Concept ID").alias("Cumsum_per_omopid")
        ).with_columns(
            (
                100 * pl.col("Cumsum_per_omopid") / pl.col("Total_per_omopid")
            ).alias("CumPercentage")
        )
        .with_columns(
            (pl.col("CumPercentage") < 90).alias("Below_threshold?")
        )
        .with_columns(
            pl.col("Below_threshold?").shift(n=1, fill_value=True).over("OMOP Concept ID").alias("Include_first_to_90percent")
        )
        .sort("rowid")
        .select(
            pl.col("OMOP Concept ID"),
            pl.col("N Records"),
            pl.col("Include_first_to_90percent")
        )
        .collect()
        .write_csv(output)
    )


if __name__ == '__main__':
    main()
