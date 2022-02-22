import sqlite3
import pymysql
import os
import pandas as pd
import numpy as np
from batch_insert import batch_insert

if os.path.exists('database.db'):  # If database exists
    os.remove('database.db')  # Delete the database
open('database.db', 'x')  # Create empty database

con = sqlite3.connect('database.db')  # Connect to the new database
# con = pymysql.connect(host='localhost', user='root', db='database.db')
csr = con.cursor()  # Set the cursor to the database

####################################################################
# Create Tables
####################################################################
# Create User table
csr.execute("CREATE TABLE User("
            "ID INT PRIMARY KEY, "
            "Username VARCHAR(500) NOT NULL, "
            "Password VARCHAR(5000) NOT NULL"
            ");")

# Create Race Lookup table
csr.execute("CREATE TABLE Lkp_Race("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Sex Lookup table
csr.execute("CREATE TABLE Lkp_Sex("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Insurance Lookup table
csr.execute("CREATE TABLE Lkp_Insurance("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create YesNo Lookup table
csr.execute("CREATE TABLE Lkp_YesNo("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Ethnicity Lookup table
csr.execute("CREATE TABLE Lkp_Ethnicity("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create ICD10 Lookup table
csr.execute("CREATE TABLE Lkp_ICD10("
            "ID INT PRIMARY KEY, "
            "Code VARCHAR(50) NOT NULL, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Assessment Color Lookup table
csr.execute("CREATE TABLE Lkp_AssessmentColor("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Discharge Status Lookup table
csr.execute("CREATE TABLE Lkp_DischargeStatus("
            "ID INT PRIMARY KEY, "
            "Description VARCHAR(500) NOT NULL"
            ");")

# Create Patient table
csr.execute("CREATE TABLE Patient("
            "ID INT PRIMARY KEY, "
            "Race INT NULL, "
            "Ethnicity INT NULL, "
            "Sex INT NULL, "
            # "ADD CONSTRAINT patientRaceConstraint "  # If we delete a race lookup, set null or update
            "FOREIGN KEY (Race) "
            "REFERENCES Lkp_Race(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT patientEthnicityConstraint "  # If we delete a ethnicity lookup, set null or update
            "FOREIGN KEY (Ethnicity)"
            "REFERENCES Lkp_Ethnciity(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT patientSexConstraint "  # If we delete a sex lookup, set null or update
            "FOREIGN KEY(Sex)"
            "REFERENCES Lkp_Sex(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

# Create Insurance table
csr.execute("CREATE TABLE Insurance("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "Encounter_Number INT NULL, "
            "Insurance_CD INT NULL, "
            # "ADD CONSTRAINT insurancePatientConstraint "  # If we delete a patient, delete or update
            "FOREIGN KEY(Subject_Id)"
            "REFERENCES Patient(ID) ON "
            "DELETE CASCADE "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT insuranceCodeConstraint "  # If we delete a insurance lookup, set null or update
            "FOREIGN KEY(Insurance_CD)"
            "REFERENCES Lkp_Insurance(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

# Create Admission table
csr.execute("CREATE TABLE Admission("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "Encounter_Number INT NULL, "
            "ICD_10 INT NULL, "
            "Visit_Reason VARCHAR(1000) NULL, "
            # "ADD CONSTRAINT admissionSubjectConstraint "  # If we delete a patient from patient table, cascade/update
            "FOREIGN KEY(Subject_Id)"
            "REFERENCES Patient(ID) ON "
            "DELETE CASCADE "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT admissionICDConstraint "  # If we delete a ICD lookup, set null or update
            "FOREIGN KEY(ICD_10)"
            "REFERENCES Lkp_ICD10(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

# Create Study Cohort table
csr.execute("CREATE TABLE StudyCohort("
            "ID INT PRIMARY KEY, "
            "Encounter_Number INT NULL, "
            "Discharge_Status INT NULL, "
            "Length_of_Stay DECIMAL(7,3) NULL, "
            "Age_at_Admission DECIMAL(7,3) NULL, "
            # "ADD CONSTRAINT studyCohortDischargeConstraint "  # If we delete a Discharge lookup, set null or update
            "FOREIGN KEY(Discharge_Status)"
            "REFERENCES Lkp_DischargeStatus(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

# Create COVID Status table
csr.execute("CREATE TABLE COVIDStatus("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "COVID_Positive_Lab_YN INT NULL, "
            "COVID_Positive_Strong_dx_YN INT NULL, "
            "COVID_Positive_Weak_dx_YN INT NULL, "
            "COVID_Positive_N3C_Phenotype_YN INT NULL, "
            # "ADD CONSTRAINT cvStatusPositiveYNConstraint "  # If we delete a YN lookup, set null or update
            "FOREIGN KEY(COVID_Positive_Lab_YN)"
            "REFERENCES Lkp_YesNo(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT cvStatusPositiveStrongYNConstraint "  # If we delete a YN lookup, set null or update
            "FOREIGN KEY(COVID_Positive_Strong_dx_YN)"
            "REFERENCES Lkp_YesNo(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT cvStatusPositiveWeakYNConstraint "  # If we delete a YN lookup, set null or update
            "FOREIGN KEY(COVID_Positive_Weak_dx_YN)"
            "REFERENCES Lkp_YesNo(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT cvStatusN3CYNConstraint "  # If we delete a YN lookup, set null or update
            "FOREIGN KEY(COVID_Positive_N3C_Phenotype_YN)"
            "REFERENCES Lkp_YesNo(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

# Create BLIS table
csr.execute("CREATE TABLE BLIS("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "Intubation_Number INT NULL, "
            "Assessment_Timepoint INT NULL, "
            "Vent_Duration DECIMAL(8,3) NULL, "
            "SOFA INT NULL, "
            "Assessment_Color_CD INT NULL, "
            # "ADD CONSTRAINT blisPatientConstraint "  # If we delete a patient, delete or update
            "FOREIGN KEY(Subject_Id)"
            "REFERENCES Patient(ID) ON "
            "DELETE CASCADE "
            "ON UPDATE CASCADE, "
            # "ADD CONSTRAINT blisAssessmentColorConstraint "  # If we delete a assessment color lkp, delete or update
            "FOREIGN KEY(Assessment_Color_CD)"
            "REFERENCES Lkp_AssessmentColor(ID) ON "
            "DELETE SET NULL "
            "ON UPDATE CASCADE"
            ");")

####################################################################
# Wrangle Data
####################################################################
reduced_race_dict = {
    ' White or Caucasian': 'White',
    ' Unknown': np.nan,
    ' Black or African American': 'Black',
    ' Black or African American ~ Other': 'Black',
    ' Black or African American ~ Unknown': 'Black',
    ' Asian ~ Unknown': 'Asian',
    ' Asian': 'Asian',
    ' Asian ~ Asian Indian': '',
    ' Other ~ White or Caucasian': 'White',
    ' Asian ~ Filipino': 'Asian',
    ' Patient Refused ~ White or Caucasian': 'White',
    ' Unknown ~ White or Caucasian': 'White',
    ' Patient Refused': np.nan,
    ' Nepalese ~ Other': 'Asian',
    ' Pakistani': '',
    ' Other ~ Unknown': np.nan,
    ' Other Pacific Islander': '',
    ' Black or African American ~ White or Caucasian': '',
    ' Asian Indian': 'Asian',
    ' Laotian ~ Other': '',
    ' Asian ~ Vietnamese': 'Asian',
    ' American Indian or Alaskan Native': 'White',
    ' American Indian or Alaskan Native ~ Black or African American': '',
    ' Guamanian': '',
    ' Asian ~ Chinese': 'Asian',
    ' Asian ~ Other': 'Asian',
    ' Indonesian': 'Asian',
    ' Palauan ~ White or Caucasian': 'White',
    ' Nepalese': '',
    ' Bangladeshi ~ White or Caucasian': '',
    ' Black or African American ~ Other ~ White or Caucasian': '',
    ' American Indian or Alaskan Native ~ Other': 'White',
    ' American Indian or Alaskan Native ~ White or Caucasian': 'White',
    ' Black or African American ~ Other ~ Unknown': '',
    ' Black or African American ~ Native Hawaiian': '',
    ' Asian Indian ~ Unknown': 'Asian',
    ' Black or African American ~ Indonesian ~ Other': '',
    ' Asian Indian ~ White or Caucasian': '',
    ' White or Caucasian ~ Yapese': '',
    ' Asian ~ White or Caucasian': '',
    ' Bhutanese ~ Other': 'Asian'
}

race_dict_lkp = {
    'American Indian or Alaska Native': 1,
    'Asian': 2,
    'Black or African American': 3,
    'Native Hawaiian or Other Pacific Islander': 4,
    'White': 5
}


reduced_ethnicity_dict = {
    ' Not Hispanic or Latino': 'Not Hispanic',
    ' Hispanic or Latino': 'Hispanic',
    ' Not Hispanic or Latino ~ Unknown': 'Not Hispanic',
    ' Unknown': np.nan,
    ' Not Hispanic or Latino ~ Patient Refused': 'Not Hispanic',
    ' Puerto Rican': 'Hispanic',
    ' Patient Refused': np.nan,
    ' Mexican American Indian': 'Hispanic',
    ' Patient Refused ~ Unknown': np.nan,
    ' Hispanic or Latino ~ Not Hispanic or Latino': np.nan,
    ' Not Hispanic or Latino ~ Uruguayan': 'Not Hispanic',
    ' Hispanic or Latino ~ Unknown': 'Hispanic',
    ' Spaniard ~ Unknown': 'Hispanic',
    ' Hispanic or Latino ~ Puerto Rican': 'Hispanic',
    ' Peruvian': 'Hispanic',
    ' Guatemalan ~ Honduran ~ Puerto Rican ~ Spaniard': 'Hispanic',
    ' Central American': 'Hispanic',
    ' Spaniard': 'Hispanic',
    ' Puerto Rican ~ Unknown': 'Hispanic',
    ' Dominican ~ Puerto Rican': 'Hispanic',
    ' Mexican American': 'Hispanic',
    ' Guatemalan': 'Hispanic',
    ' South American Indian': 'Hispanic',
    ' Central American Indian': 'Hispanic'
}

ethnicity_dict_lkp = {
    'Hispanic': 1,
    'Not Hispanic': 2,
}

reduced_insurance_dict = {
    'Commercial': 'Private',
    'Out of Area BC/BS': '',
    'Government Other': 'Government',
    'Medicare': 'Medicare',
    'Medicaid': 'Medicaid',
    'Medicare Advantage': 'Medicare',
    'Medicaid Managed Care': 'Medicaid',
    'Excellus': 'Private',
    'MVA': 'MVA',
    'Aetna': 'Private',
    'MVP': 'MVP',
    "Worker's Comp": "Worker's Comp",
    'Institutional': 'Private',
    'UNIVERA SENIOR CHOICE MEDICARE': 'Medicare',
    'MEDICARE PART A AND B': 'Medicare',
    'MVP PREMIER INDIVIDUAL': 'Private',
    'EXCELLUS': 'Private',
    'UNITED HEALTHCARE MEDICAID': 'Medicad'
}

insurance_dict_lkp = {
    'Private': 1,
    'Medicare': 2,
    'Medicaid': 3
}

reduced_assessment_color_dict = {
    'RED': 'Red',
    'YELLOW': 'Yellow',
    'BLUE': 'Blue'
}

assessment_color_dict_lkp = {
    'Red': 1,
    'Yellow': 2,
    'Blue': 3
}


discharge_status_dict_lkp = {
    'Home or Self Care': 1,
    'Patient Expired': 2,
    'To Home Health Org Care': 3,
    'To Jail / Law Enforcement Facility': 4,
    'To SNF (Skilled Nursing)': 5,
    'To Inpatient Rehab Facility or Unit': 6,
    'Left against medical advice': 7,
    'To Hospice/Home Care': 8,
    'To Psychiatric Hospital or Unit': 9,
    'To Short Term Acute Care Hosp': 10,
    'To Hospice/Medical Facility': 11,
    'To LTC Facility (Long Term Care)': 12,
    'Sent to SMH': 13,
    'Sent to HH': 14,
    'To Short Term General Hospital for Inpatient Care with Planned Hospital Readmission': 15,
    'To Inpatient Rehab Facility or Unit with Planned Hospital Readmission': 16,
    'To Other Facility not otherwise defined': 17,
    'To ICF (Intermediate Care)': 18,
    'To Federal Hospital': 19,
    "To Designated Cancer Ctr or Children's Hospital with Planned Hospital Readmission": 20,
    'Still Inpatient': 21,
    'To Psychiatric Hospital or Unit with Planned Hospital Readmission': 22
}

yesno_dict_lkp = {
    'Yes': 1,
    'No': 2
}

sex_dict_lkp = {
    'M': 1,
    'F': 2
}

db_sex_lkp_df = pd.DataFrame(sex_dict_lkp.items(), columns=['ID', 'Description'])
db_race_lkp_df = pd.DataFrame(race_dict_lkp.items(), columns=['ID', 'Description'])
db_insurance_lkp_df = pd.DataFrame(insurance_dict_lkp.items(), columns=['ID', 'Description'])
db_ethnicity_lkp_df = pd.DataFrame(ethnicity_dict_lkp.items(), columns=['ID', 'Description'])
db_yesno_lkp_df = pd.DataFrame(yesno_dict_lkp.items(), columns=['ID', 'Description'])
db_assessment_color_lkp_df = pd.DataFrame(assessment_color_dict_lkp.items(), columns=['ID', 'Description'])
db_discharge_status_lkp_df = pd.DataFrame(discharge_status_dict_lkp.items(), columns=['ID', 'Description'])


study_cohort_df = pd.read_excel('VAP_DeID_2022-01-12 from CTSI.xlsx', sheet_name='Study_Cohort_DeID')
study_cohort_df['Race'] = study_cohort_df['Race'].map(reduced_race_dict).map(race_dict_lkp)
study_cohort_df['Ethnicity'] = study_cohort_df['Ethnicity'].map(reduced_ethnicity_dict).map(ethnicity_dict_lkp)
study_cohort_df['Sex'] = study_cohort_df['Sex'].map(sex_dict_lkp)
study_cohort_df['Discharge_Status'] = study_cohort_df['Discharge_Status'].map(discharge_status_dict_lkp)

db_patient_df = study_cohort_df[['SubjectID', 'Race', 'Ethnicity', 'Sex']]
db_patient_df = db_patient_df.drop_duplicates()
db_patient_df.columns = ['ID', 'Race', 'Ethnicity', 'Sex']

db_study_cohort_df = study_cohort_df[['seq', 'Encounter_Number', 'Discharge_Status', 'Length_of_Stay (days)',
                                      'Age_at_Admission']]
db_study_cohort_df.columns = ['ID', 'Encounter_Number',  'Discharge_Status', 'Length_of_Stay_Days', 'Age_at_Admission']

admission_df = pd.read_excel('VAP_DeID_2022-01-12 from CTSI.xlsx', sheet_name='Admission_dXs_DeID')
db_icd10_lkp_df = admission_df[['ICD10_dX(s)', 'dX_Name']].copy().drop_duplicates().reset_index(drop=True)
db_icd10_lkp_df['ID'] = db_icd10_lkp_df.index
db_icd10_lkp_df = db_icd10_lkp_df[['ID', 'ICD10_dX(s)', 'dX_Name']]
db_icd10_lkp_df.columns = ['ID', 'Code', 'Description']

icd10_dict_lkp = dict(zip(db_icd10_lkp_df['Code'], db_icd10_lkp_df['ID']))
admission_df['ICD10_dX(s)'] = admission_df['ICD10_dX(s)'].map(icd10_dict_lkp)
db_admission_df = admission_df[['seq', 'SubjectID', 'Encounter_Number', 'ICD10_dX(s)', 'Reason_Visit']]
db_admission_df.columns = ['ID', 'Subject_Id', ' Encounter_Number', 'ICD_10', 'Visit_Reason']

insurance_df = pd.read_excel('VAP_DeID_2022-01-12 from CTSI.xlsx', sheet_name='Insurance_DeID')
insurance_df['Insurance'] = insurance_df['Insurance'].map(reduced_insurance_dict).map(insurance_dict_lkp)
db_insurance_df = insurance_df.copy()
db_insurance_df.columns = ['ID', 'Subject_Id', 'Encounter_Number', 'Insurance_CD']

blis_df = pd.read_excel('VAP_DeID_2022-01-12 from CTSI.xlsx', sheet_name='BLIS_ASSESSMENT_DeID')
blis_df['assessment_color'] = blis_df['assessment_color'].map(reduced_assessment_color_dict).\
    map(assessment_color_dict_lkp)
db_blis_df = blis_df[['seq_num', 'SubjectID', 'intubation_number', 'assessment_timepoint', 'vent_duration (hours)',
                      'sofa_score', 'assessment_color']]
db_blis_df.columns = ['ID', 'Subject_Id', 'Intubation_Number', 'Assessment_Timepoint', 'Vent_Duration', 'SOFA',
                      'Assessment_Color_CD']

covid_df = pd.read_excel('VAP_DeID_2022-01-12 from CTSI.xlsx', sheet_name='COVID_Status_DeID')
covid_df['covid_positive_lab'] = covid_df['covid_positive_lab'] .map(yesno_dict_lkp)
covid_df['covid_positive_strong_dx'] = covid_df['covid_positive_strong_dx'].map(yesno_dict_lkp)
covid_df['covid_positive_weak_dx'] = covid_df['covid_positive_weak_dx'].map(yesno_dict_lkp)
covid_df['covid_positive_weak_dx'] = covid_df['covid_positive_weak_dx'].map(yesno_dict_lkp)
db_covid = covid_df.copy()
db_covid.columns = ['ID', 'Subject_Id', 'COVID_Positive_Lab_YN', 'COVID_Positive_Strong_dx_YN',
                    'COVID_Positive_Weak_dx_YN', 'COVID_Positive_N3C_Phenotype_YN']

####################################################################
# Batch Insert Data
####################################################################
batch_insert(con, db_sex_lkp_df, 'Lkp_Sex')
batch_insert(con, db_race_lkp_df, 'Lkp_Race')
batch_insert(con, db_insurance_lkp_df, 'Lkp_Insurance')
batch_insert(con, db_ethnicity_lkp_df, 'Lkp_Ethnicity')
batch_insert(con, db_yesno_lkp_df, 'Lkp_YesNo')
batch_insert(con, db_assessment_color_lkp_df, 'Lkp_AssessmentColor')
batch_insert(con, db_discharge_status_lkp_df, 'Lkp_DischargeStatus')

batch_insert(con, db_patient_df, 'Patient')
batch_insert(con, db_study_cohort_df, 'StudyCohort')
batch_insert(con, db_admission_df, 'Admission')
batch_insert(con, db_insurance_df, 'Insurance')
batch_insert(con, db_blis_df, 'BLIS')
batch_insert(con, db_covid, 'COVIDStatus')
