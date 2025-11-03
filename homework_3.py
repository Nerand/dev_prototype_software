# app.py
from typing import List, Tuple, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, conint
from sqlalchemy import create_engine, Column, Integer, String, select, func
from sqlalchemy.orm import declarative_base, Session
import csv
import os

Base = declarative_base()

class Student(Base):
    __tablename__ = "students"
    id      = Column(Integer, primary_key=True, autoincrement=True)
    surname = Column(String(100), index=True)
    name    = Column(String(100), index=True)
    faculty = Column(String(200), index=True)
    course  = Column(String(200), index=True)
    grade   = Column(Integer)

class StudentsDAO:
    def __init__(self, db_url: str = "sqlite:///students_simple.db"):
        self.engine = create_engine(db_url, echo=False, future=True)
        Base.metadata.create_all(self.engine)

    # INSERT одной записи
    def insert(self, surname: str, name: str, faculty: str, course: str, grade: int) -> int:
        with Session(self.engine) as s:
            rec = Student(surname=surname, name=name, faculty=faculty, course=course, grade=int(grade))
            s.add(rec)
            s.commit()
            s.refresh(rec)
            return rec.id

    # SELECT всех
    def select_all(self) -> List[Student]:
        with Session(self.engine) as s:
            return list(s.scalars(select(Student).order_by(Student.id)))

    # Загрузка из CSV
    def load_from_csv(self, csv_path: str, encoding: str = "utf-8-sig") -> int:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(csv_path)

        keys = {
            "surname": {"Фамилия", "surname"},
            "name":    {"Имя", "name"},
            "faculty": {"Факультет", "faculty"},
            "course":  {"Курс", "course"},
            "grade":   {"Оценка", "grade"},
        }

        inserted = 0
        with Session(self.engine) as s, open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return 0

            header = set(reader.fieldnames)
            colmap = {}
            for norm, variants in keys.items():
                found = next((h for h in header if h in variants), None)
                if not found:
                    raise ValueError(f"В CSV нет колонки '{norm}' (ожидались: {variants})")
                colmap[norm] = found

            batch = []
            for row in reader:
                try:
                    grade_val = int(row[colmap["grade"]])
                except Exception:
                    continue
                rec = Student(
                    surname=row[colmap["surname"]].strip(),
                    name=row[colmap["name"]].strip(),
                    faculty=row[colmap["faculty"]].strip(),
                    course=row[colmap["course"]].strip(),
                    grade=grade_val,
                )
                batch.append(rec)
                if len(batch) >= 1000:
                    s.add_all(batch); s.commit(); inserted += len(batch); batch.clear()

            if batch:
                s.add_all(batch); s.commit(); inserted += len(batch)

        return inserted

    # Аналитика
    def get_students_by_faculty(self, faculty: str) -> List[Tuple[str, str]]:
        with Session(self.engine) as s:
            stmt = (
                select(Student.surname, Student.name)
                .where(Student.faculty == faculty)
                .distinct()
                .order_by(Student.surname, Student.name)
            )
            return s.execute(stmt).all()

    def get_unique_courses(self) -> List[str]:
        with Session(self.engine) as s:
            stmt = select(Student.course).distinct().order_by(Student.course)
            return [r[0] for r in s.execute(stmt).all()]

    def get_avg_grade_by_faculty(self, faculty: str) -> Optional[float]:
        with Session(self.engine) as s:
            stmt = select(func.avg(Student.grade)).where(Student.faculty == faculty)
            val = s.execute(stmt).scalar()
            return float(val) if val is not None else None


app = FastAPI(title="Students API (simple)")
dao = StudentsDAO()

# Pydantic-модель для вставки
class StudentIn(BaseModel):
    surname: str
    name: str
    faculty: str
    course: str
    grade: conint(ge=0, le=100)

@app.get("/")
async def root():
    return {"msg": "Students API: POST /students, GET /students, POST /load_csv?path=students.csv, /faculties/{name}/students, /courses, /faculties/{name}/avg"}

@app.post("/students")
async def create_student(payload: StudentIn):
    new_id = dao.insert(
        surname=payload.surname,
        name=payload.name,
        faculty=payload.faculty,
        course=payload.course,
        grade=payload.grade,
    )
    return {"status": "ok", "id": new_id}

@app.get("/students")
async def list_students():
    rows = dao.select_all()
    return [
        {
            "id": r.id,
            "surname": r.surname,
            "name": r.name,
            "faculty": r.faculty,
            "course": r.course,
            "grade": r.grade,
        }
        for r in rows
    ]

@app.post("/load_csv")
async def load_csv(path: str = Query("students.csv", description="Путь к CSV-файлу")):
    try:
        count = dao.load_from_csv(path)
        return {"status": "ok", "inserted": count, "path": path}
    except FileNotFoundError:
        raise HTTPException(404, f"Файл не найден: {path}")
    except ValueError as e:
        raise HTTPException(400, str(e))

@app.get("/faculties/{faculty}/students")
async def students_by_faculty(faculty: str):
    pairs = dao.get_students_by_faculty(faculty)
    return [{"surname": s, "name": n} for s, n in pairs]

@app.get("/courses")
async def unique_courses():
    return {"courses": dao.get_unique_courses()}

@app.get("/faculties/{faculty}/avg")
async def avg_by_faculty(faculty: str):
    val = dao.get_avg_grade_by_faculty(faculty)
    if val is None:
        return {"faculty": faculty, "avg_grade": None, "message": "записей нет"}
    return {"faculty": faculty, "avg_grade": round(val, 2)}

if __name__ == "__main__":
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True
    )
