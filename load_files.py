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


class USSchools(SchoolsDataSet):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.column_rename_map = {
            "SCHOOL_YEAR": "SchoolYear",
            "STATENAME": "StateName",
            "ST": "State",
            "SCH_NAME": "Name",
            "LEA_NAME": "LEAName",
            "STATE_AGENCY_NO": "StateAgencyNumber",
            "UNION": "Union",
            "ST_LEAID": "StateLEAID",
            "ST_SCHID": "StateSchoolID",
            "NCESSCH": "NCESSchoolID",
            "SCHID": "SchoolID",
            "MSTREET1": "MailingAddressLine1",
            "MSTREET2": "MailingAddressLine2",
            "MSTREET3": "MailingAddressLine3",
            "MCITY": "MailingCity",
            "MSTATE": "MailingState",
            "MZIP": "MailingZip",
            "MZIP4": "MailingZip4",
            "LSTREET1": "PhysicalAddressLine1",
            "LSTREET2": "PhysicalAddressLine2",
            "LSTREET3": "PhysicalAddressLine3",
            "LCITY": "PhysicalAddressCity",
            "LSTATE": "PhysicalAddressState",
            "LZIP": "PhysicaAddressZip",
            "LZIP4": "PhysicalAddressZip4",
            "PHONE": "Phone",
            "WEBSITE": "Website",
            "SY_STATUS": "SYStatus",
            "SY_STATUS_TEXT": "SYStatusDescription",
            "UPDATED_STATUS": "UpdatedStatus",
            "UPDATED_STATUS_TEXT": "UpdatedStatusText",
            "EFFECTIVE_DATE": "EffectiveDate",
            "SCH_TYPE_TEXT": "SchoolTypeText",
            "SCH_TYPE": "SchoolType",
            "RECON_STATUS": "Reconstituted",
            "OUT_OF_STATE_FLAG": "OutOfState",
            "CHARTER_TEXT": "Charter",
            "CHARTAUTH1": "CharterAuthorizer1",
            "CHARTAUTHN1": "ChartherAuthorizerN1",
            "CHARTAUTH2": "CharterAuthorizer2",
            "CHARTAUTHN2": "CharterAuthorizerN2",
            "NOGRADES": "NoGrades",
            "G_PK_OFFERED": "PK",
            "G_KG_OFFERED": "KG",
            "G_1_OFFERED": "01",
            "G_2_OFFERED": "02",
            "G_3_OFFERED": "03",
            "G_4_OFFERED": "04",
            "G_5_OFFERED": "05",
            "G_6_OFFERED": "06",
            "G_7_OFFERED": "07",
            "G_8_OFFERED": "08",
            "G_9_OFFERED": "09",
            "G_10_OFFERED": "10",
            "G_11_OFFERED": "11",
            "G_12_OFFERED": "12",
            "G_13_OFFERED": "13",
            "G_UG_OFFERED": "Ungraded",
            "G_AE_OFFERED": "AdultEducation",
            "GSLO": "GradeOfferedLow",
            "GSHI": "GradeOfferedHigh",
            "LEVEL": "Level",
            "IGOFFERED": "IGOffered",
        }
        self.us_schools_df = self.open_file()
        self.states_df = self._create_states_df()
        self._transform_school_details()

    def _create_states_df(self) -> pd.DataFrame:
        states_df = self.us_schools_df[["State", "StateName"]]
        states_df.drop_duplicates(inplace=True)
        states_df["StateName"] = states_df["StateName"].str.title()
        states_df["StateID"] = range(1, len(states_df) + 1)
        cols = ["StateID"] + [col for col in states_df.columns if col != "StateID"]
        states_df = states_df[cols]
        return states_df

    def _transform_school_details(self):
        states_map = self.states_df.set_index("State")["StateID"].to_dict()

        # Add StateID to the column
        self.us_schools_df["StateID"] = self.us_schools_df["State"].map(states_map)

        # Drop unneeded columns
        self.us_schools_df.drop(columns=["StateName", "FIPST"], inplace=True)

        # Reorder Columns
        cols_to_move = ["SchoolID", "SchoolYear", "StateID"]
        other_cols = [c for c in self.us_schools_df.columns if c not in cols_to_move]
        self.us_schools_df = self.us_schools_df[cols_to_move + other_cols]

        # Convert Boolean Columns
        obj_cols = self.us_schools_df.select_dtypes(include=["object"]).columns

        bool_cols = []
        for col in obj_cols:
            unique_values = self.us_schools_df[col].dropna().unique()
            # Check if the unique values are a subset of {'Yes', 'No'}
            if set(unique_values).issubset({"Yes", "No", "Not reported", "Not applicable"}):
                bool_cols.append(col)

        # 3. Map "Yes" to True and "No" to False
        mapping = {"Yes": "Y", "No": "N", "Not reported": "U", "Not applicable": "NA"}
        self.us_schools_df[bool_cols] = self.us_schools_df[bool_cols].replace(mapping)


class USSchoolsCharacteristicsData(SchoolsDataSet):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.column_rename_map = {
            "SCHOOL_YEAR": "SchoolYear",
            "SCHID": "SchoolID",
            "SHARED_TIME": "SharedTime",
            "NSLP_STATUS": "NSLPStatus",
            "NSLP_STATUS_TEXT": "NSLPDescription",
            "VIRTUAL": "Virtual",
            "VIRTUAL_TEXT": "VirtualDescription",
        }
        self.us_schools_df = self.open_file()
        self._transform_us_schools_data()

    def _transform_us_schools_data(self):
        # Drop extra columns
        self.us_schools_df = self.us_schools_df[
            [v for v in self.column_rename_map.values()]
        ]


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
    def _transform_private_schools(self):
        pass


if __name__ == "__main__":
    us_schools = USSchools(
        file_path=os.path.join("data", "us_schools_detail_24_25.csv")
    )
    us_schools_characteristics = USSchoolsCharacteristicsData(
        file_path=os.path.join("data", "us_schools.csv")
    )
    us_school_demographics = USSchoolDemographicsData(
        file_path=os.path.join("data", "us_schools_demographics_24_25.csv"),
    )
    private_schools = PrivateSchoolsData(
        os.path.join("data", "private_schools_21_22.csv")
    )

    # Send to SQL Database
    dfs_for_sql = {
        "SchoolCharacteristics": us_schools_characteristics.us_schools_df,
        "SchoolDetails": us_schools.us_schools_df,
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
