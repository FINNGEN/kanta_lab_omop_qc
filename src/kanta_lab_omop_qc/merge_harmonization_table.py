from argparse import ArgumentParser
from pathlib import Path

import polars as pl


DESCRIPTION = (
    "I take the Kanta lab harmonization table from Google Sheet as a CSV, and "
    "the FinRegistry Kanta stats on the lab values. From this I add the Top 10 "
    "lab values for each row of the harmonization table."
)


def main():
    args = cli_init()

    merged_table = add_stats_to_table(
        args.harmonization_table,
        args.finregistry_stats
    )

    merged_table.write_csv(
        args.output,
    )


def cli_init():
    parser = ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        "--harmonization-table",
        help="path to the harmonization table (CSV)",
        type=Path,
        required=True
    )

    parser.add_argument(
        "--finregistry-stats",
        help="path to FinRegistry stats on Kanta lab values (CSV)",
        type=Path,
        required=True
    )

    parser.add_argument(
        "--output",
        help="path to output file of merged table (CSV)",
        type=Path,
        required=True
    )

    args = parser.parse_args()

    return args


def add_stats_to_table(table, stats):
    dataf_table = pl.read_csv(
        table,
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
            stats,
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

    merged_table = (
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
    )

    return merged_table


if __name__ == '__main__':
    main()
