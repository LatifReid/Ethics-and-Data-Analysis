from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import Patient, get_db, init_db

app = FastAPI(title="Healthcare API")

class PatientModel(BaseModel):
    name: str
    age: int
    condition: str
    admission_date: str

@app.on_event("startup")
def startup_event():
    init_db()

@app.post("/patients/")
def create_patient(patient: PatientModel, db: Session = Depends(get_db)):
    if patient.age < 0 or patient.age > 120:
        raise HTTPException(status_code=422, detail="Age must be between 0 and 120")
    db_patient = Patient(**patient.dict())
    db.add(db_patient)
    db.commit()
    db.refresh(db_patient)
    return db_patient

@app.get("/patients/")
def get_patients(db: Session = Depends(get_db)):
    return db.query(Patient).all()

@app.get("/patients/{patient_id}")
def get_patient(patient_id: int, db: Session = Depends(get_db)):
    patient = db.query(Patient).filter(Patient.id == patient_id).first()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")
    return patient

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from database import Patient, get_db, init_db

app = FastAPI(title="Healthcare API")

class PatientModel(BaseModel):
    name: str
    age: int
    condition: str
    admission_date: str

@app.on_event("startup")
def startup_event():
    init_db()

@app.post("/patients/")
def create_patient(patient: PatientModel, db: Session = Depends(get_db)):
    if patient.age < 0 or patient.age > 120:
        raise HTTPException(status_code=422, detail="Age must be between 0 and 120")
    db_patient = Patient(**patient.dict())
    db.add(db_patient)
    db.commit()
    db.refresh(db_patient)
    return db_patient

@app.get("/patients/")
def get_patients(db: Session = Depends(get_db)):
    return db.query(Patient).all()

@app.get("/patients/{patient_id}")
def get_patient(patient_id: int, db: Session = Depends(get_db)):
    patient = db.query(Patient).filter(Patient.id == patient_id).first()
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")
    return patient