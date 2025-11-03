# uvicorn homework_2:app --reload
import os
import re
import json
import uvicorn
from uuid import uuid4
from datetime import date
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, field_validator

app = FastAPI()

CYRILLIC_NAME_RE = r"^[А-ЯЁ][а-яё]+$"
PHONE_RE = r"^\+?\d{10,15}$"

class Appeal(BaseModel):
    surname: str
    name: str
    birthdate: date
    phone: str
    email: EmailStr

    @field_validator("surname")
    def validate_surname(cls, v: str) -> str:
        if not re.match(CYRILLIC_NAME_RE, v):
            raise ValueError("Фамилия: с заглавной буквы, только кириллица (пример: Иванов)")
        return v

    @field_validator("name")
    def validate_name(cls, v: str) -> str:
        if not re.match(CYRILLIC_NAME_RE, v):
            raise ValueError("Имя: с заглавной буквы, только кириллица (пример: Иван)")
        return v

    @field_validator("phone")
    def validate_phone(cls, v: str) -> str:
        if not re.match(PHONE_RE, v):
            raise ValueError("Телефон: допустим формат + и 10–15 цифр (пример: +79991234567)")
        return v

@app.get("/")
async def root():
    return {"msg": "POST /appeals — создать обращение"}

@app.post("/appeals")
async def create_appeal(appeal: Appeal):
        os.makedirs("data", exist_ok=True)
        file_id = str(uuid4())
        path = os.path.join("data", f"{file_id}.json")
        payload = {
            "surname": appeal.surname,
            "name": appeal.name,
            "birthdate": appeal.birthdate.isoformat(),
            "phone": appeal.phone,
            "email": str(appeal.email),
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка записи файла: {e}")

        return {"status": "ok", "id": file_id, "file": path, "data": payload}

if __name__ == "__main__":
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True
    )
