
###########    Hospital Database Management System using MySQL and Python ####################
import mysql.connector
import random
import pandas as pd
from datetime import date, timedelta
from IPython.display import display
from sqlalchemy import create_engine
import os
import time

# ================== MySQL Connection ==================
conn = mysql.connector.connect(
    host="localhost",
    user="Neeraj",
    password="Neeraj@_240203"
)
cursor = conn.cursor()

# ================== Create Database ==================
cursor.execute("CREATE DATABASE IF NOT EXISTS HospitalDB")
cursor.execute("USE HospitalDB")

# ================== Patient Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Patient (
    PatientID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL,
    DateOfBirth DATE,
    Gender VARCHAR(10) CHECK (Gender IN ('Male','Female')),
    Address VARCHAR(100),
    PhoneNumber VARCHAR(15) UNIQUE,
    Email VARCHAR(100) UNIQUE
)
""")

# ================== Department Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Department (
    DepartmentID INT PRIMARY KEY AUTO_INCREMENT,
    DepartmentName VARCHAR(100) NOT NULL UNIQUE,
    Location VARCHAR(100)
)
""")

# ================== Doctor Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Doctor (
    DoctorID INT PRIMARY KEY AUTO_INCREMENT,
    Name VARCHAR(100) NOT NULL,
    Specialization VARCHAR(100),
    PhoneNumber VARCHAR(15) UNIQUE,
    Email VARCHAR(100) UNIQUE,
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Department(DepartmentID)
)
""")

# ================== Appointment Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Appointment (
    AppointmentID INT PRIMARY KEY AUTO_INCREMENT,
    PatientID INT,
    DoctorID INT,
    AppointmentDate DATE,
    AppointmentTime TIME,
    Status VARCHAR(50) DEFAULT 'Scheduled',
    FOREIGN KEY (PatientID) REFERENCES Patient(PatientID),
    FOREIGN KEY (DoctorID) REFERENCES Doctor(DoctorID)
)
""")

# ================== Prescription Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Prescription (
    PrescriptionID INT PRIMARY KEY AUTO_INCREMENT,
    AppointmentID INT,
    MedicationDetails VARCHAR(255),
    Dosage VARCHAR(100),
    Duration VARCHAR(100),
    FOREIGN KEY (AppointmentID) REFERENCES Appointment(AppointmentID)
)
""")

# ================== Billing Table ==================
cursor.execute("""
CREATE TABLE IF NOT EXISTS Billing (
    BillID INT PRIMARY KEY AUTO_INCREMENT,
    PatientID INT,
    Amount DECIMAL(10,2),
    BillDate DATE,
    PaymentStatus VARCHAR(50) DEFAULT 'Pending',
    FOREIGN KEY (PatientID) REFERENCES Patient(PatientID)
)
""")

print("✅ Tables with constraints created")

# ================== Indexes (RE-RUN SAFE) ==================
cursor.execute("""
SELECT COUNT(1)
FROM information_schema.statistics
WHERE table_schema = 'HospitalDB'
AND table_name = 'Patient'
AND index_name = 'idx_patient_name'
""")
if cursor.fetchone()[0] == 0:
    cursor.execute("CREATE INDEX idx_patient_name ON Patient(Name)")

cursor.execute("""
SELECT COUNT(1)
FROM information_schema.statistics
WHERE table_schema = 'HospitalDB'
AND table_name = 'Appointment'
AND index_name = 'idx_appointment_date'
""")
if cursor.fetchone()[0] == 0:
    cursor.execute("CREATE INDEX idx_appointment_date ON Appointment(AppointmentDate)")

print("✅ Indexes created or already exist")

# ================== Views ==================
cursor.execute("""
CREATE OR REPLACE VIEW PatientAppointmentsView AS
SELECT 
    p.PatientID,
    p.Name AS PatientName,
    a.AppointmentDate,
    a.Status
FROM Patient p
JOIN Appointment a ON p.PatientID = a.PatientID
""")

cursor.execute("""
CREATE OR REPLACE VIEW DoctorDepartmentView AS
SELECT 
    d.Name AS DoctorName,
    d.Specialization,
    dp.DepartmentName
FROM Doctor d
JOIN Department dp ON d.DepartmentID = dp.DepartmentID
""")

print("✅ Views created")
conn.commit()

# ================== Insert Random Patient Data ==================
cursor.execute("DELETE FROM Patient")
conn.commit()

male_names = ["Rahul", "Amit", "Rohit", "Vikas", "Anil", "Suresh"]
female_names = ["Anita", "Sunita", "Priya", "Neha", "Pooja"]
last_names = ["Sharma", "Verma", "Singh", "Gupta", "Patel"]
cities = ["Delhi", "Mumbai", "Chennai", "Bangalore", "Pune"]

def random_dob():
    start = date(1960, 1, 1)
    end = date(2010, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_phone():
    return "9" + str(random.randint(100000000, 999999999))

for _ in range(1000):
    gender = random.choice(["Male", "Female"])
    fname = random.choice(male_names if gender == "Male" else female_names)
    lname = random.choice(last_names)

    cursor.execute("""
        INSERT INTO Patient
        (Name, DateOfBirth, Gender, Address, PhoneNumber, Email)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (
        f"{fname} {lname}",
        random_dob(),
        gender,
        random.choice(cities),
        random_phone(),
        f"{fname.lower()}{random.randint(1000,9999)}@gmail.com"
    ))

conn.commit()
print("✅ 1000 random patients inserted")

# ================== SQLAlchemy Engine (Fix Warning) ==================
engine = create_engine(
    "mysql+mysqlconnector://Neeraj:Neeraj%40_240203@localhost/HospitalDB"
)

df = pd.read_sql("SELECT * FROM Patient", engine)

# ================== Display Table ==================
print("\n📋 FULL PATIENT TABLE")
display(df)

print("\n📊 Gender Distribution")
display(df['Gender'].value_counts())

# ================== Export to Excel (G Drive SAFE) ==================
folder_path = "G:/DBMS"
os.makedirs(folder_path, exist_ok=True)

timestamp = time.strftime("%Y%m%d_%H%M%S")
excel_path = f"{folder_path}/HospitalDB_Complete_Assignment_{timestamp}.xlsx"

df.to_excel(excel_path, index=False)
print(f"📁 Excel exported successfully to: {excel_path}")

# ================== Close Connections ==================
cursor.close()
conn.close()
print("🔒 Database connection closed")
✅ Tables with constraints created
✅ Indexes created or already exist
✅ Views created
✅ 1000 random patients inserted

📋 FULL PATIENT TABLE
PatientID	Name	DateOfBirth	Gender	Address	PhoneNumber	Email
0	14001	Vikas Singh	1960-03-17	Male	Mumbai	9146533012	vikas6381@gmail.com
1	14002	Sunita Gupta	2003-10-08	Female	Pune	9665978300	sunita6733@gmail.com
2	14003	Sunita Singh	1989-11-29	Female	Delhi	9812692447	sunita8799@gmail.com
3	14004	Anita Singh	1975-08-19	Female	Delhi	9244877746	anita4766@gmail.com
4	14005	Neha Singh	1999-03-13	Female	Mumbai	9195854500	neha6286@gmail.com
...	...	...	...	...	...	...	...
995	14996	Vikas Singh	2008-10-20	Male	Mumbai	9234552923	vikas3057@gmail.com
996	14997	Pooja Verma	1984-03-27	Female	Pune	9184973646	pooja5764@gmail.com
997	14998	Sunita Sharma	1988-02-23	Female	Bangalore	9676914758	sunita6059@gmail.com
998	14999	Vikas Sharma	2007-05-15	Male	Pune	9602205043	vikas7396@gmail.com
999	15000	Rohit Patel	2003-08-18	Male	Chennai	9652353491	rohit2230@gmail.com
1000 rows × 7 columns

📊 Gender Distribution
Gender
Female    503
Male      497
Name: count, dtype: int64
📁 Excel exported successfully to: G:/DBMS/HospitalDB_Complete_Assignment_20260118_155011.xlsx
🔒 Database connection closed
 