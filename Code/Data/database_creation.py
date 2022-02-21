import sqlite3
# import pymysql
import os
import pandas as pd

if os.path.exists('database.db'):  # If database exists
    os.remove('database.db')  # Delete the database
open('database.db', 'x')  # Create empty database

con = sqlite3.connect('database.db')  # Connect to the new database
# con = pymysql.connect(host='localhost', user='root', db='database.db')
csr = con.cursor()  # Set the cursor to the database

####################################################################
####################### Create Tables ##############################
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
            "COVID_Positive_Weak_dx_YN INT NULL, "
            "COVID_Positive_N3C_Phenotype_YN INT NULL, "
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
            "Age_at_Admission DECIMAL(7,3) NULL"
            ");")

# Create COVID Status table
csr.execute("CREATE TABLE COVIDStatus("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "COVID_Positive_Lab_YN INT NULL, "
            "COVID_Positive_Strong_dx_YN INT NULL, "
            "COVID_Positive_Weak_dx_YN INT NULL, "
            "COVID_Positive_N3C_Phenotype_YN INT NULL"
            ");")

# Create BLIS table
csr.execute("CREATE TABLE BLIS("
            "ID INT PRIMARY KEY, "
            "Subject_Id INT NULL, "
            "Intubation_Number INT NULL, "
            "Assessment_Timepoint INT NULL, "
            "Vent_Duration DECIMAL(8,3) NULL, "
            "SOFA INT NULL, "
            "Assessment_Color_CD INT NULL"
            ");")
