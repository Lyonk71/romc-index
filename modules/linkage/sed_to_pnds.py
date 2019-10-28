import pandas as pd
import pandas_dedupe

from modules.linkage.project_functions import standardize_licnum, low_confidence


def sed_to_pnds():
    # ### Import datasets

    # import sed
    df_sed = pd.read_csv(
        "input_datasets/LICENSE_NON_CONF_ACTIVE.csv", dtype=str, low_memory=False
    )
    df_sed = df_sed[["LICENSE_NUMBER", "NAME", "AGE_END_OF_YEAR"]]
    df_sed = df_sed.drop_duplicates()
    df_sed = df_sed.dropna()

    # import pnds
    df_pnds = pd.read_csv(
        "intermediate_datasets/clustered_addresses.csv", dtype=str, low_memory=False
    )
    df_pnds = df_pnds.rename(
        {"cluster id": "pnds_cluster_id", "confidence": "pnds confidence"}, axis=1
    )

    df_pnds_subset = df_pnds[["StdFirstName", "StdLastName", "licnum"]].copy()

    # ### match headers

    # rename pnds fields to match sed fields
    df_pnds_subset = df_pnds_subset.rename({"licnum": "LICENSE_NUMBER"}, axis=1)
    df_pnds_subset["NAME"] = df_pnds["StdLastName"] + " " + df_pnds["StdFirstName"]
    df_pnds_subset = df_pnds_subset[["NAME", "LICENSE_NUMBER"]].copy()

    # ### standardize license number

    df_sed = standardize_licnum(df_sed, "LICENSE_NUMBER")
    df_pnds_subset = standardize_licnum(df_pnds_subset, "LICENSE_NUMBER")
    df_pnds = standardize_licnum(df_pnds, "licnum")

    # ### Keep only unique license numbers from pnds_subset

    df_pnds_subset = df_pnds_subset.drop_duplicates(subset=["LICENSE_NUMBER"]).copy()

    # ### link dataframes

    # create dataset labels
    df_pnds_subset["dataset"] = "pnds"
    df_sed["dataset"] = "sed"

    # link pnds & sed
    dfq = pandas_dedupe.link_dataframes(
        df_pnds_subset,
        df_sed,
        [("LICENSE_NUMBER", "Exact"), "NAME"],
        config_name="dedupe_settings/sed_to_pnds",
    )
    # separate datasets
    df_pnds_subset = dfq[dfq["dataset"] == "pnds"].copy()
    df_sed = dfq[dfq["dataset"] == "sed"].copy()

    # remove low confidence scores from each dataset
    df_pnds_subset["confidence"] = df_pnds_subset.apply(
        lambda x: low_confidence(x["confidence"], x["confidence"]), axis=1
    )
    df_pnds_subset["cluster id"] = df_pnds_subset.apply(
        lambda x: low_confidence(x["cluster id"], x["confidence"]), axis=1
    )

    df_sed["confidence"] = df_sed.apply(
        lambda x: low_confidence(x["confidence"], x["confidence"]), axis=1
    )
    df_sed["cluster id"] = df_sed.apply(
        lambda x: low_confidence(x["cluster id"], x["confidence"]), axis=1
    )

    # remove extraneous SED information (for clean merge) - remove null columns
    df_sed = df_sed.dropna(subset=["cluster id"]).copy()
    df_sed = df_sed[["LICENSE_NUMBER", "AGE_END_OF_YEAR"]].copy()
    df_pnds_subset = df_pnds_subset.drop(columns=["AGE_END_OF_YEAR"]).copy()

    # rename sed for merge with pnds
    df_sed = df_sed.rename(
        {"LICENSE_NUMBER": "licnum", "AGE_END_OF_YEAR": "age"}, axis=1
    ).copy()

    # merge age onto original pcmh file
    df_pnds = df_pnds.merge(df_sed, how="left", on="licnum")

    df_pnds.to_csv("output_datasets/clustered_with_age.csv")
