# uvicorn homework_5:app --reload
from typing import List, Tuple, Optional, Dict
from fastapi import FastAPI, HTTPException, Query, Path, Depends, Header
from pydantic import BaseModel, conint
from sqlalchemy import create_engine, Column, Integer, String, select, func, UniqueConstraint
from sqlalchemy.orm import declarative_base, Session
import csv
import os
import uvicorn
import hashlib
import secrets

Base = declarative_base()

class Student(Base):
    __tablename__ = "students"
    id      = Column(Integer, primary_key=True, autoincrement=True)
    surname = Column(String(100), index=True)
    name    = Column(String(100), index=True)
    faculty = Column(String(200), index=True)
    course  = Column(String(200), index=True)
    grade   = Column(Integer)

class User(Base):
    __tablename__ = "users"
    id       = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), nullable=False)
    password = Column(String(256), nullable=False)
    salt     = Column(String(64), nullable=False)
    __table_args__ = (UniqueConstraint("username", name="uix_username"),)

class StudentsDAO:
    def __init__(self, db_url: str = "sqlite:///students_simple.db"):
        self.engine = create_engine(db_url, echo=False, future=True)
        Base.metadata.create_all(self.engine)

    def insert(self, surname: str, name: str, faculty: str, course: str, grade: int) -> int:
        with Session(self.engine) as s:
            rec = Student(surname=surname, name=name, faculty=faculty, course=course, grade=int(grade))
            s.add(rec)
            s.commit()
            s.refresh(rec)
            return rec.id

    def select_all(self) -> List[Student]:
        with Session(self.engine) as s:
            return list(s.scalars(select(Student).order_by(Student.id)))

    def get_by_id(self, student_id: int) -> Optional[Student]:
        with Session(self.engine) as s:
            return s.get(Student, student_id)

    def update(self, student_id: int, data: dict) -> bool:
        with Session(self.engine) as s:
            rec = s.get(Student, student_id)
            if not rec:
                return False
            for k, v in data.items():
                setattr(rec, k, v)
            s.commit()
            return True

    def delete(self, student_id: int) -> bool:
        with Session(self.engine) as s:
            rec = s.get(Student, student_id)
            if not rec:
                return False
            s.delete(rec)
            s.commit()
            return True

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

class UsersDAO:
    def __init__(self, engine):
        self.engine = engine

    def create_user(self, username: str, password: str) -> int:
        salt = secrets.token_hex(16)
        pwd = hashlib.sha256((salt + password).encode()).hexdigest()
        with Session(self.engine) as s:
            if s.execute(select(User).where(User.username == username)).scalar_one_or_none():
                raise ValueError("username_taken")
            u = User(username=username, password=pwd, salt=salt)
            s.add(u)
            s.commit()
            s.refresh(u)
            return u.id

    def verify_user(self, username: str, password: str) -> Optional[int]:
        with Session(self.engine) as s:
            u = s.execute(select(User).where(User.username == username)).scalar_one_or_none()
            if not u:
                return None
            pwd = hashlib.sha256((u.salt + password).encode()).hexdigest()
            if pwd != u.password:
                return None
            return u.id

app = FastAPI(title="Students API with Auth")
dao = StudentsDAO()
users = UsersDAO(dao.engine)
SESSIONS: Dict[str, int] = {}

class StudentIn(BaseModel):
    surname: str
    name: str
    faculty: str
    course: str
    grade: conint(ge=0, le=100)

class StudentOut(StudentIn):
    id: int

class StudentUpdate(BaseModel):
    surname: Optional[str] = None
    name: Optional[str] = None
    faculty: Optional[str] = None
    course: Optional[str] = None
    grade: Optional[conint(ge=0, le=100)] = None

class AuthIn(BaseModel):
    username: str
    password: str

def get_current_user(authorization: Optional[str] = Header(None)) -> int:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "unauthorized")
    token = authorization.split(" ", 1)[1]
    uid = SESSIONS.get(token)
    if not uid:
        raise HTTPException(401, "unauthorized")
    return uid

auth = FastAPI()

@app.get("/")
async def root():
    return {"auth": "/auth/register, /auth/login, /auth/logout", "api": "protected"}

@app.post("/auth/register")
async def register(payload: AuthIn):
    try:
        uid = users.create_user(payload.username, payload.password)
        return {"status": "ok", "user_id": uid}
    except ValueError:
        raise HTTPException(400, "username_taken")

@app.post("/auth/login")
async def login(payload: AuthIn):
    uid = users.verify_user(payload.username, payload.password)
    if not uid:
        raise HTTPException(401, "invalid_credentials")
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = uid
    return {"status": "ok", "token": token, "user_id": uid}

@app.post("/auth/logout")
async def logout(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        return {"status": "ok"}
    token = authorization.split(" ", 1)[1]
    SESSIONS.pop(token, None)
    return {"status": "ok"}

@app.post("/students")
async def create_student(payload: StudentIn, user_id: int = Depends(get_current_user)):
    new_id = dao.insert(
        surname=payload.surname,
        name=payload.name,
        faculty=payload.faculty,
        course=payload.course,
        grade=payload.grade,
    )
    return {"status": "ok", "id": new_id}

@app.get("/students", response_model=List[StudentOut])
async def list_students(user_id: int = Depends(get_current_user)):
    rows = dao.select_all()
    return [StudentOut(id=r.id, surname=r.surname, name=r.name, faculty=r.faculty, course=r.course, grade=r.grade) for r in rows]

@app.get("/students/{student_id}", response_model=StudentOut)
async def get_student(student_id: int = Path(..., ge=1), user_id: int = Depends(get_current_user)):
    rec = dao.get_by_id(student_id)
    if not rec:
        raise HTTPException(404, "not found")
    return StudentOut(id=rec.id, surname=rec.surname, name=rec.name, faculty=rec.faculty, course=rec.course, grade=rec.grade)

@app.put("/students/{student_id}")
async def put_student(student_id: int, payload: StudentIn, user_id: int = Depends(get_current_user)):
    ok = dao.update(student_id, payload.dict())
    if not ok:
        raise HTTPException(404, "not found")
    return {"status": "ok", "id": student_id}

@app.patch("/students/{student_id}")
async def patch_student(student_id: int, payload: StudentUpdate, user_id: int = Depends(get_current_user)):
    data = {k: v for k, v in payload.dict().items() if v is not None}
    if not data:
        return {"status": "noop"}
    ok = dao.update(student_id, data)
    if not ok:
        raise HTTPException(404, "not found")
    return {"status": "ok", "id": student_id}

@app.delete("/students/{student_id}")
async def delete_student(student_id: int, user_id: int = Depends(get_current_user)):
    ok = dao.delete(student_id)
    if not ok:
        raise HTTPException(404, "not found")
    return {"status": "ok", "id": student_id}

@app.post("/load_csv")
async def load_csv(path: str = Query("students.csv"), user_id: int = Depends(get_current_user)):
    try:
        count = dao.load_from_csv(path)
        return {"status": "ok", "inserted": count, "path": path}
    except FileNotFoundError:
        raise HTTPException(404, f"Файл не найден: {path}")
    except ValueError as e:
        raise HTTPException(400, str(e))

@app.get("/faculties/{faculty}/students")
async def students_by_faculty(faculty: str, user_id: int = Depends(get_current_user)):
    pairs = dao.get_students_by_faculty(faculty)
    return [{"surname": s, "name": n} for s, n in pairs]

@app.get("/courses")
async def unique_courses(user_id: int = Depends(get_current_user)):
    return {"courses": dao.get_unique_courses()}

@app.get("/faculties/{faculty}/avg")
async def avg_by_faculty(faculty: str, user_id: int = Depends(get_current_user)):
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
