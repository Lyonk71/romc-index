import pandas as pd
import numpy as np
import pandas_npi

from fuzzywuzzy import fuzz
import pandas_usaddress
import probablepeople


def preprocess_pnds():
    # ### Create relevant dictionaries & functions

    specialty_df = pd.read_csv("input_datasets/pnds_specialty_codes.csv")
    specialty_df["Code"] = specialty_df["Code"].apply(int)
    spec_dict = dict(
        zip(
            specialty_df["Code"], specialty_df["Provider Specialty/Service Description"]
        )
    )

    language_df = pd.read_csv("input_datasets/language_codes.csv")
    language_dict = dict(zip(language_df["CODE"], language_df["LANGUAGE NAME"]))

    planb_df = pd.read_csv("input_datasets/planb.csv")
    planb_dict = dict(zip(planb_df["Planname"], planb_df["planlabel"]))

    # designation_dict = {1: "PCP", 2: "Specialist", 3: "Both"}
    gender_dict = {1: "Male", 2: "Female"}

    # ### Import PNDS providers

    fields = [
        "npi",
        "sitenpi",
        "licnum",
        "gender",
        "fein",
        "sitename",
        "StdFirstName",
        "StdLastName",
        #     'primdesg',
        "provtype",
        "primspec",
        "secdspec",
        "lang1",
        "lang2",
        "Address1_std",
        "Address2_std",
        "City_std",
        "TrueCountyName",
        "State_std",
        "Zip_std",
        "Latitude",
        "Longitude",
        "Planname",
        "phone",
        "email",
    ]

    df = pd.read_csv("input_datasets/pnds.csv", usecols=fields)

    # ### There are some issues with FEINs that must be corrected:
    # * Mixed types, so convert to string
    # * On string conversion, some float values exist. remove with regex
    # * Some dashes and letters exist remove with regex
    # * Some datasets had 1 or 2 leading zeroes missing. Add them in
    # * Keep as string. Should always be string due to leading zeroes
    # * Remove all values like 999999999, and 888888888

    df["fein"] = df["fein"].astype(str)

    df["fein"] = df["fein"].replace("[.][0-9]+$", "", regex=True)
    df["fein"] = df["fein"].replace("[^\d]", "", regex=True)

    def correct_leading_zeroes(x):
        if len(x) == 9:
            return x
        elif len(x) == 8:
            return "0" + x
        elif len(x) == 7:
            return "00" + x
        elif len(x) < 7:
            return np.nan

    df["fein"] = df["fein"].apply(lambda x: correct_leading_zeroes(x))

    def remove_filler_tins(x):
        if x == "999999999":
            return np.nan
        elif x == "888888888":
            return np.nan
        elif x == "777777777":
            return np.nan
        elif x == "666666666":
            return np.nan
        elif x == "555555555":
            return np.nan
        elif x == "444444444":
            return np.nan
        elif x == "333333333":
            return np.nan
        elif x == "222222222":
            return np.nan
        elif x == "111111111":
            return np.nan
        else:
            return x

    df["fein"] = df["fein"].apply(lambda x: remove_filler_tins(x))
    df = df.dropna(subset=["fein"])

    # ### Extract physicians, nurse practitioners, and physician assistants

    df = df[
        (df["provtype"] == 1)
        | (df["provtype"] == 12)
        | (df["provtype"] == 2)
        | (df["provtype"] == 23)
    ].copy()
    provtype_dict = {
        1: "physician",
        12: "physician",
        2: "nurse practitioner",
        23: "physician assistant",
    }
    df["provtype"] = df["provtype"].apply(lambda x: provtype_dict.get(x, x))

    # Extract physicians only
    df = df[df["provtype"] == "physician"]

    # ### fix provider npi
    # * validate_npi converts filler null values into NaN

    df = pandas_npi.validate(df, "npi")
    df = df[df["nppes_status"] == "active"]
    df = df[df["nppes_type"] == "provider"]

    df = df.drop(columns=["nppes_deactivation_date"]).copy()

    df = df.rename(
        {
            "nppes_type": "provider_nppes_type",
            "nppes_status": "provider_nppes_status",
            "nppes_name": "provider_nppes_name",
        },
        axis=1,
    )

    # ### fix facility npi
    # * validate_npi converts filler null values into NaN

    df = pandas_npi.validate(df, "sitenpi")

    df = df.rename(
        {
            "nppes_type": "facility_nppes_type",
            "nppes_status": "facility_nppes_status",
            "nppes_name": "facility_nppes_name",
        },
        axis=1,
    )

    def remove_incorrect_npi(facility_nppes_type, sitenpi):
        if facility_nppes_type == "facility":
            return sitenpi
        else:
            return np.nan

    df["sitenpi"] = df.apply(
        lambda x: remove_incorrect_npi(x["facility_nppes_type"], x["sitenpi"]), axis=1
    )

    # ### Fix licnum
    # * Convert to six digit numerical string
    # * leading digits need to be added
    # * licnum needs to be canonicalized (more than one being listed per npi)

    df["licnum"] = df["licnum"].astype(str)
    df["licnum"] = df["licnum"].replace("[.][0-9]+$", "", regex=True)
    df["licnum"] = df["licnum"].replace("[^\d]", "", regex=True)

    def correct_leading_zeroes(x):
        if len(x) == 6:
            return x
        elif len(x) == 5:
            return "0" + x
        elif len(x) == 4:
            return "00" + x
        elif len(x) == 3:
            return "000" + x
        elif len(x) == 2:
            return "0000" + x
        elif len(x) < 2:
            return np.nan

    df["licnum"] = df["licnum"].apply(lambda x: correct_leading_zeroes(x))

    def remove_filler_licnum(x):
        if x == "999999":
            return np.nan
        elif x == "888888":
            return np.nan
        elif x == "777777":
            return np.nan
        elif x == "666666":
            return np.nan
        elif x == "555555":
            return np.nan
        elif x == "444444":
            return np.nan
        elif x == "333333":
            return np.nan
        elif x == "222222":
            return np.nan
        elif x == "111111":
            return np.nan
        else:
            return x

    df["licnum"] = df["licnum"].apply(lambda x: remove_filler_licnum(x))

    # Canonicalize licnum

    df["licnum_count"] = df["licnum"].copy()

    df_test = df[["npi", "licnum", "licnum_count"]].copy()
    df_grp = df_test.groupby(["npi", "licnum"], as_index=False).count()

    # Get highest frequency licnum
    df_grp["licnum_max"] = df_grp.groupby("npi")["licnum_count"].transform("max")

    def keep_max_frequency(licnum_count, licnum_max, licnum):
        if licnum_count == licnum_max:
            return licnum
        else:
            return np.nan

    df_grp["licnum"] = df_grp.apply(
        lambda x: keep_max_frequency(x["licnum_count"], x["licnum_max"], x["licnum"]),
        axis=1,
    )

    df_grp = df_grp[["npi", "licnum"]].dropna().copy()

    df = df.drop(columns=["licnum"]).copy()

    df = df.merge(df_grp, how="left", on="npi")

    df = df.drop(columns=["licnum_count"]).copy()
    del df_grp
    del df_test

    # ### Pickle

    import pickle

    pickle.dump(df, open("pickles/preprocess_pnds_pickle_1.p", "wb"))

    # df = pickle.load( open( "pickles/preprocess_pnds_pickle_1.py", "rb" ) )

    df["lang2"].value_counts()

    # ### fix zip

    df["Zip_std"] = df["Zip_std"].astype(str)
    df["Zip_std"] = df["Zip_std"].replace("[.][0-9]+$", "", regex=True)
    df["Zip_std"] = df["Zip_std"].replace("[^\d]", "", regex=True)

    def correct_leading_zeroes(x):
        if len(x) == 5:
            return x
        elif len(x) == 4:
            return "0" + x
        elif len(x) < 4:
            return np.nan

    df["Zip_std"] = df["Zip_std"].apply(lambda x: correct_leading_zeroes(x))

    # ### Add latlog
    # * some latitude/longitude values are switched, and must be corrected

    def swap_latlong(lat, lon):

        if lon > lat:
            return (lat, -1 * lon)
        elif lon < lat:
            return (lon, -1 * lat)
        else:
            pass

    df["Latitude"] = df["Latitude"].abs()
    df["Longitude"] = df["Longitude"].abs()
    df["Latitude"] = df["Latitude"].apply(lambda x: round(float(x), 5))
    df["Longitude"] = df["Longitude"].apply(lambda x: round(float(x), 5))
    df["latlong"] = df.apply(
        lambda x: swap_latlong(x["Latitude"], x["Longitude"]), axis=1
    )

    df = df.dropna(axis=1, how="all")

    # ### Drop addresses values

    df = df.dropna(subset=["Zip_std"])
    df = df.dropna(subset=["StdFirstName"])
    df = df.dropna(subset=["StdLastName"])

    # ### Convert code values to integers

    df["primspec"] = df["primspec"].apply(float)
    df["secdspec"] = df["secdspec"].apply(float)

    # create provider designation
    def provider_designation(primspec, secdspec, primary_care_list):
        if primspec in primary_care_list:
            return 1
        elif secdspec in primary_care_list:
            return 1
        else:
            return np.nan

    df['provider_designation'] = df.apply(lambda x: provider_designation(x['primspec'], x['secdspec'], [60.0, 50.0, 776.0, 55.0, 56.0, 150.0, 58.0, 182.0, 620.0, 621.0]), axis=1)


    # df['primdesg'] = df['primdesg'].apply(int)
    df["gender"] = df["gender"].apply(int)

    # ### Identify incorrect provider names

    df["StdFirstName"] = df["StdFirstName"].str.lower()
    df["StdLastName"] = df["StdLastName"].str.lower()
    df["pnds_name"] = df["StdFirstName"] + " " + df["StdLastName"]
    df["name_score"] = df.apply(
        lambda x: fuzz.partial_ratio(x["pnds_name"], x["provider_nppes_name"]), axis=1
    )
    df = df[df["provider_nppes_type"] == "provider"]
    df = df[df["name_score"] > 45]
    df = df.drop(columns=["pnds_name", "name_score"]).copy()

    # ### Use dictionaries to label various attribues
    df["primspec"] = df["primspec"].apply(lambda x: spec_dict.get(x, x))
    df["secdspec"] = df["secdspec"].apply(lambda x: spec_dict.get(x, x))

    # df['primdesg'] = df['primdesg'].apply(lambda x: designation_dict.get(x,x))
    df["gender"] = df["gender"].apply(lambda x: gender_dict.get(x, x))
    df["lang1"] = df["lang1"].apply(lambda x: language_dict.get(x, x))
    df["lang2"] = df["lang2"].apply(lambda x: language_dict.get(x, x))

    # ### Create secondary_language field, remove lang1 and lang2

    df["lang1"] = df["lang1"].astype(str)
    df["lang1"] = df["lang1"].replace({"English": np.nan, "999": np.nan, "nan": np.nan})
    df["lang2"] = df["lang2"].astype(str)
    df["lang2"] = df["lang2"].replace({"English": np.nan, "999": np.nan, "nan": np.nan})

    def extract_secondary_language(lang1, lang2):
        try:
            if np.isnan(lang1) == True:
                return lang2
            else:
                return lang1
        except:
            return lang1

    df["secondary_language"] = df.apply(
        lambda x: extract_secondary_language(x["lang1"], x["lang2"]), axis=1
    )

    # ### Pickle

    import pickle

    pickle.dump(df, open("pickles/preprocess_pnds_pickle_2.p", "wb"))

    # df = pickle.load( open( "pickles/preprocess_pnds_pickle_2.p", "rb" ) )

    # ### Drop address sitenames

    df["sitename"] = df["sitename"].str.lower()

    df = pandas_usaddress.tag(df, ["sitename"], granularity="medium")

    df[["AddressNumber", "StreetName", "StreetNameSuffix"]] = df[
        ["AddressNumber", "StreetName", "StreetNameSuffix"]
    ].replace({np.nan: None})

    def sitename_update(sitename, StreetName, StreetNameSuffix):
        if StreetName != None or StreetNameSuffix != None:
            return np.nan
        else:
            return sitename

    df["sitename"] = df.apply(
        lambda x: sitename_update(
            x["sitename"], x["StreetName"], x["StreetNameSuffix"]
        ),
        axis=1,
    )

    # ### Drop people sitenames

    def tag_ppl(x):
        try:
            return probablepeople.tag(x.lower())[1]
        except:
            return np.nan

    df["probablepeople"] = df["sitename"].apply(lambda x: tag_ppl(x))

    def sitename_update(x, y):
        if y != "Person":
            return x
        else:
            return np.nan

    def sitename_realname(sitename, firstname, lastname):
        try:
            if sitename == firstname + " " + lastname:
                return np.nan
            else:
                return sitename
        except:
            return sitename

    df["sitename"] = df.apply(
        lambda x: sitename_update(x["sitename"], x["probablepeople"]), axis=1
    )
    df["sitename"] = df.apply(
        lambda x: sitename_realname(x["sitename"], x["StdFirstName"], x["StdLastName"]),
        axis=1,
    )

    # ### Pickle

    import pickle

    pickle.dump(df, open("pickles/preprocess_pnds_pickle_3.p", "wb"))

    # df = pickle.load( open( "pickles/preprocess_pnds_pickle_3.p", "rb" ) )

    # ### Remove duplicate columns & rows

    df = df.drop(
        columns=[
            "AddressNumber",
            "PlaceName",
            "StateName",
            "StreetName",
            "StreetNamePreDirectional",
            "StreetNamePostDirectional",
            "ZipCode",
            "StreetNamePrefix",
            "StreetNameSuffix",
            "USPSBox",
            "OccupancySuite",
            "probablepeople",
        ]
    )

    df = df.drop_duplicates()

    # # Test plan frequency

    # ### Parse Addresses, add latlong field for dedupe, drop null fields caused by parsing

    # address_list = ['Address1_std', 'Address2_std', 'City_std', 'State_std', 'Zip_std']
    # df = pandas_usaddress.tag(df, address_list, granularity='medium', standardize=True)
    # df = df.drop(columns=address_list)

    # df['Latitude'] = df['Latitude'].apply(lambda x: round(float(x),5))
    # df['Longitude'] = df['Longitude'].apply(lambda x: round(float(x),5))
    # df['latlong'] = df.apply(lambda x: (x['Latitude'], x['Longitude']), axis=1)

    # df = df.dropna(axis=1, how='all')

    # ## Frequency of plans

    # ### physician fein location id

    df["physfeinloc"] = (
        df["npi"].astype(str) + df["fein"].astype(str) + df["latlong"].astype(str)
    )

    # ### Kate's Codes

    df["planb"] = df["Planname"].apply(lambda x: planb_dict.get(x, x))

    df = df.drop_duplicates(subset=["physfeinloc", "planb"])

    # ### Count the number of plans that agree on a physician work location

    df_test = df[["physfeinloc", "planb"]]
    df_grp = df_test.groupby(["physfeinloc"], as_index=False).count()

    # Planname now represents the count of plans that agree on the physicians location
    df_grp = df_grp.rename({"planb": "Plancount"}, axis=1)

    # ### Add the plancount to the provider_subset data

    df = df.merge(df_grp, how="left", on="physfeinloc")

    # ### Remove records with plan count of 1

    df = df[df["Plancount"] != 1]

    # ### Remove duplicate records

    df = df.drop_duplicates(
        subset=["npi", "fein", "Address1_std", "City_std", "Zip_std"]
    ).copy()
    df = df.drop_duplicates(subset=["npi", "fein", "latlong"]).copy()

    # Drop columns used for plan frequency
    df = df.drop(columns=["Plancount", "planb", "physfeinloc"])

    # ## Frequency of latlong

    # df_latlong = pd.DataFrame(df['latlong'].value_counts())
    # df_latlong = df_latlong.reset_index()
    # df_latlong = df_latlong.rename({'latlong': 'latlong_count', 'index': 'latlong'}, axis=1)
    # df = df.merge(df_latlong, how='left', on='latlong')

    # ### Export

    df.to_csv("intermediate_datasets/provider_subset.csv", index=False)
