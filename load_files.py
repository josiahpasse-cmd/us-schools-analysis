import pandas as pd
import os
import sqlite3


class SchoolsDataSet:
    def __init__(self, file_path: str):
        self.column_rename_map = {}
        self.file_path = file_path

    def open_file(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path)
        df.rename(columns=self.column_rename_map, inplace=True)
        return df


class USSchoolsData(SchoolsDataSet):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.column_rename_map = {
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
        self.us_schools_df = self.open_file()
        self.states_df = self._create_states_df()
        self._transform_us_schools_data()

    def _create_states_df(self) -> pd.DataFrame:
        states_df = self.us_schools_df[["State", "Description"]]
        states_df.drop_duplicates(inplace=True)
        states_df["Description"] = states_df["Description"].str.title()
        states_df["StateID"] = range(1, len(states_df) + 1)
        cols = ["StateID"] + [col for col in states_df.columns if col != "StateID"]
        states_df = states_df[cols]
        return states_df

    def _transform_us_schools_data(self):
        states_map = self.states_df.set_index("State")["StateID"].to_dict()

        # Add StateID to the column
        self.us_schools_df["StateID"] = self.us_schools_df["State"].map(states_map)

        # Drop the extra column
        self.us_schools_df.drop(columns="Description", inplace=True)

        # Reorder Columns
        us_schools_column_order = [
            "SchoolID",
            "SchoolYear",
            "FIPST",
            "StateID",
            "State",
            "Name",
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
        self.us_schools_df = self.us_schools_df[us_schools_column_order]


class USSchoolDemographicsData(SchoolsDataSet):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.column_rename_map = {
            "SCHID": "SchoolID",
            "SCHOOL_YEAR": "SchoolYear",
            "GRADE": "Grade",
            "RACE_ETHNICITY": "RaceEthnicitiy",
            "SEX": "Gender",
            "STUDENT_COUNT": "StudentCount",
            "TOTAL_INDICATOR": "TotalIndicator",
            "DMS_FLAG": "DMSFlag",
        }
        self.us_school_demographics_df = self.open_file()
        self._transform_schools_demographics_data()

    def _transform_schools_demographics_data(self):
        # Remove unnecessary columns
        self.us_school_demographics_df = self.us_school_demographics_df[
            [value for value in self.column_rename_map.values()]
        ]

        # Convert StudentCount to int
        self.us_school_demographics_df["StudentCount"] = self.us_school_demographics_df[
            "StudentCount"
        ].fillna(0)
        self.us_school_demographics_df["StudentCount"] = self.us_school_demographics_df[
            "StudentCount"
        ].astype(int)


class PrivateSchoolsData(SchoolsDataSet):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.column_rename_map = {}  # TODO build out the column_rename map
        self.private_schools_df = self.open_file()
        self._transform_private_schools()

    # TODO Build out this function to transform the private schools into a useful dataframe
    def _transform_private_schools():
        pass


us_schools = USSchoolsData(file_path=os.path.join("data", "us_schools.csv"))
us_school_demographics = USSchoolDemographicsData(
    file_path=os.path.join("data", "us_schools_demographics_24_25.csv"),
)
private_schools = PrivateSchoolsData(os.path.join("data", "private_schools_21_22.csv"))

# Send to SQL Database
dfs_for_sql = {
    "Schools": us_schools.us_schools_df,
    "SchoolDemographics": us_school_demographics.us_school_demographics_df,
    "States": us_schools.states_df,
    "PrivateSchools": private_schools.private_schools_df,
}


def write_to_sql(db: str, dfs_for_sql: dict[str, pd.DataFrame]) -> None:
    connection = sqlite3.Connection(db)

    # Run to_sql
    for table, df in dfs_for_sql.items():
        df.to_sql(table, con=connection, index=False, if_exists="replace")


write_to_sql(db="Schools.db", dfs_for_sql=dfs_for_sql)
