from fastapi.testclient import TestClient
from main import app
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, Patient
import pytest


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def test_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    yield db
    db.close()


def test_create_patient(client, test_db):
    response = client.post(
        "/patients/",
        json={
            "name": "Bob Wilson",
            "age": 50,
            "condition": "Asthma",
            "admission_date": "2025-04-01",
        },
    )
    assert response.status_code == 200
    assert response.json()["name"] == "Bob Wilson"


def test_create_patient_invalid_age(client):
    response = client.post(
        "/patients/",
        json={
            "name": "Bob Wilson",
            "age": 150,
            "condition": "Asthma",
            "admission_date": "2025-04-01",
        },
    )
    assert response.status_code == 422


def test_get_patients(client, test_db):
    client.post(
        "/patients/",
        json={
            "name": "Bob Wilson",
            "age": 50,
            "condition": "Asthma",
            "admission_date": "2025-04-01",
        },
    )
    response = client.get("/patients/")
    assert response.status_code == 200
    assert len(response.json()) >= 1