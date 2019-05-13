import numpy as np

# standardize_licnum function
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


def standardize_licnum(df, licnum):
    # convert to string - remove numbers after decimal - remove non-numeric chars
    df[licnum] = df[licnum].astype(str)
    df[licnum] = df[licnum].replace("[.][0-9]+$", "", regex=True)
    df[licnum] = df[licnum].replace("[^\d]", "", regex=True)

    # correct leading zeroes
    df[licnum] = df[licnum].apply(lambda x: correct_leading_zeroes(x))
    df[licnum] = df[licnum].apply(lambda x: remove_filler_licnum(x))
    return df


# remove low confidence score (dedupe)
def low_confidence(field, confidence):
    if np.isnan(confidence) == True:
        field = np.nan
        return np.nan
    if confidence < 0.2:
        field = np.nan
        return field
    else:
        return field
