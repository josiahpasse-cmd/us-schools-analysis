import pandas as pd
import os
import sqlite3

schools_rename_map = {
    "SCHOOL_YEAR": "SchoolYear",
    "ST": "State",
    "STATENAME": "Description",
    "SCH_NAME": "Name",
    "ST_LEAID": "StateLEAID",
    "ST_SCHID": "StateSchoolID",
    "NCESSCH": "NCESSchoolID",
    "SCHID": "SchoolID",
    "SHARED_TIME": "SharedTime",
    "NSLP_STATUS": "NSLPStatus",
    "NSLP_STATUS_TEXT": "NSLPDescription",
    "VIRTUAL": "Virtual",
    "VIRTUAL_TEXT": "VirtualDescription",
}

# open the Public School Data file
us_schools_df = pd.read_csv(os.path.join("data", "us_schools.csv"))
us_schools_df.rename(columns=schools_rename_map, inplace=True)

# States df
states_df = us_schools_df[["State", "Description"]]
states_df.drop_duplicates(inplace=True)
states_df["Description"] = states_df["Description"].str.title()
states_df["StateID"] = range(1, len(states_df) + 1)
cols = ["StateID"] + [col for col in states_df.columns if col != "StateID"]
states_df = states_df[cols]

states_map = states_df.set_index("State")["StateID"].to_dict()

# Add StateID to the column
us_schools_df["StateID"] = us_schools_df["State"].map(states_map)

# Drop the extra column
us_schools_df.drop(columns="Description", inplace=True)

# Reorder Columns
us_schools_column_order = [
    "SchoolID",
    "SchoolYear",
    "FIPST",
    "StateID",
    "State",
    "SchoolName",
    "STATE_AGENCY_NO",
    "UNION",
    "StateLEAID",
    "LEAID",
    "StateSchoolID",
    "NCESSchoolID",
    "SharedTime",
    "NSLPStatus",
    "NSLPDescription",
    "Virtual",
    "VirtualDescription",
]
us_schools_df = us_schools_df[us_schools_column_order]

# Open public school characteristics
us_school_demographics = pd.read_csv(
    os.path.join("data", "us_schools_demographics.csv")
)

# Rename columns for demographics
us_school_demographics_rename_map = {
    "SCHID": "SchoolID",
    "SCHOOL_YEAR": "SchoolYear",
    "GRADE": "Grade",
    "RACE_ETHNICITY": "RaceEthnicitiy",
    "SEX": "Gender",
    "STUDENT_COUNT": "StudentCount",
    "TOTAL_INDICATOR": "TotalIndicator",
    "DMS_FLAG": "DMSFlag",
}

us_school_demographics = us_school_demographics.rename(
    columns=us_school_demographics_rename_map
)

# Remove unnecessary columns
us_school_demographics = us_school_demographics[
    [value for value in us_school_demographics_rename_map.values()]
]

# Convert StudentCount to int
us_school_demographics["StudentCount"] = us_school_demographics["StudentCount"].fillna(
    0
)
us_school_demographics["StudentCount"] = us_school_demographics["StudentCount"].astype(
    int
)

# Open private schools file
private_schools_df = pd.read_csv(os.path.join("data", "private_schools.csv"))

# Send to SQL Database
dfs_for_sql = {
    "Schools": us_schools_df,
    "SchoolDemographics": us_school_demographics,
    "States": states_df,
    "PrivateSchools": private_schools_df,
}

connection = sqlite3.Connection("Schools.db")

# Run to_sql
for table, df in dfs_for_sql.items():
    df.to_sql(table, con=connection, index=False, if_exists="replace")

# us_schools_df.to_sql("Schools", con=connection, index=False, if_exists="replace")
# states_df.to_sql("States", con=connection, index=False, if_exists="replace")
# private_schools_df.to_sql(
#     "PrivateSchools", con=connection, index=False, if_exists="replace"
# )
