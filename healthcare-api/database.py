from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
import csv
from typing import Generator

Base = declarative_base()
engine = create_engine('sqlite:///patients.db', echo=False)
SessionLocal = sessionmaker(bind=engine)


class Patient(Base):
    __tablename__ = 'patients'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    age = Column(Integer)
    condition = Column(String)
    admission_date = Column(String)


def init_db() -> None:
    """Create tables and load initial data from patients.csv if DB is empty."""
    Base.metadata.create_all(bind=engine)

    # If there is a patients.csv present and the table is empty, populate it.
    try:
        with SessionLocal() as session:
            count = session.query(Patient).count()
            if count == 0:
                try:
                    with open('patients.csv', 'r', newline='') as f:
                        csv_reader = csv.DictReader(f)
                        for row in csv_reader:
                            # Convert numeric fields as needed
                            if 'age' in row and row['age']:
                                row['age'] = int(row['age'])
                            session.add(Patient(**row))
                        session.commit()
                except FileNotFoundError:
                    # No initial CSV — that's fine for tests or fresh DB
                    pass
    except Exception:
        # If init fails, don't crash import; let the app surface runtime errors.
        pass


def get_db() -> Generator[sessionmaker, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()