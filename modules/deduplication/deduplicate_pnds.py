import pandas as pd
import pandas_dedupe
import pandas_usaddress
import numpy as np
import pandas_npi


def deduplicate_pnds():
    df = pd.read_csv("intermediate_datasets/provider_subset.csv", dtype=str)

    ### Finds PNDS duplicates

    dfq = pandas_dedupe.dedupe_dataframe(
        df,
        [
            ("fein", "Exact"),
            "Address1_std",
            ("City_std", "Exact"),
            ("Zip_std", "Exact"),
            ("latlong", "LatLong"),
        ],
        config_name="dedupe_settings/deduplicate_pnds",
        canonicalize=True,
    )

    import pickle

    pickle.dump(dfq, open("pickles/deduplicate_pnds_pickle_1.p", "wb"))

    # dfq = pickle.load(open("pickles/deduplicate_pnds_pickle_1.p", "rb"))

    drop_fields = [
        "fein - canonical",
        "Latitude",
        "Longitude",
        "Planname",
        "provider_nppes_type",
        "provider_nppes_status",
        "provider_nppes_name",
        "facility_nppes_type",
        "facility_nppes_status",
        "facility_nppes_name",
        "npi - canonical",
        "provtype - canonical",
        "gender - canonical",
        "fein - canonical",
        "Latitude - canonical",
        "Longitude - canonical",
        "StdFirstName - canonical",
        "StdLastName - canonical",
        "Address2_std - canonical",
        "Planname - canonical",
        "provider_nppes_type - canonical",
        "provider_nppes_status - canonical",
        "provider_nppes_name - canonical",
        "facility_nppes_type - canonical",
        "facility_nppes_status - canonical",
        "licnum - canonical",
        "secondary_language - canonical",
        "facility_nppes_name - canonical",
        "primspec - canonical",
        "secdspec - canonical",
        "provider_designation",
    ]

    dfq = dfq.drop(columns=drop_fields, axis=1)

    df_match = dfq.dropna(subset=["confidence"])
    df_nomatch = dfq[dfq["confidence"].isnull()]

    original_fields = [
        "sitename",
        "latlong",
        "City_std",
        "TrueCountyName",
        "State_std",
        "Zip_std",
        "Address1_std",
    ]

    canonical_dict = {
        "sitename - canonical": "sitename",
        "latlong - canonical": "latlong",
        "City_std - canonical": "City_std",
        "TrueCountyName - canonical": "TrueCountyName",
        "State_std - canonical": "State_std",
        "Zip_std - canonical": "Zip_std",
        "Address1_std - canonical": "Address1_std",
    }

    df_match = df_match.drop(columns=original_fields)
    df_match = df_match.rename(columns=canonical_dict)

    canonical_fields = [
        "sitename - canonical",
        "latlong - canonical",
        "City_std - canonical",
        "TrueCountyName - canonical",
        "State_std - canonical",
        "Zip_std - canonical",
        "Address1_std - canonical",
    ]

    df_nomatch = df_nomatch.drop(columns=canonical_fields)

    dfq = pd.concat([df_match, df_nomatch], sort=True)

    dfq = dfq.sort_values("cluster id")

    dfq = dfq.drop_duplicates(subset=["cluster id", "npi"])

    dfq = pandas_npi.validate(dfq, "sitenpi - canonical")

    dfq = dfq.drop(columns=["nppes_type", "nppes_status"])

    dfq["nppes_name"] = dfq["nppes_name"].replace({"invalid": np.nan})

    dfq.to_csv("intermediate_datasets/clustered_addresses.csv", index=False)
