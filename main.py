# uvicorn main:app --reload
import uvicorn 
from fastapi import FastAPI

app = FastAPI()

@app.get("/add/{a}/{b}")
async def add(a: float, b: float):
    return {"operation": "add", "result": a + b}

@app.get("/sub/{a}/{b}")
async def sub(a: float, b: float):
    return {"operation": "sub", "result": a - b}

@app.get("/mul/{a}/{b}")
async def mul(a: float, b: float):
    return {"operation": "mul", "result": a * b}

@app.get("/div/{a}/{b}")
async def div(a: float, b: float):
    if b == 0:
        return {"error": "деление на ноль"}
    return {"operation": "div", "result": a / b}

''' дальше для работы со сложными выражениями'''

@app.get("/make_expr/{a}/{op}/{b}")
async def make_expr(a: str, op: str, b: str):
    global CURRENT_EXPR

    if op not in ["+", "-", "*", "/"]:
        return {"error": "неизвестная операция"}

    part = f"({a}{op}{b})"

    if CURRENT_EXPR:
        CURRENT_EXPR = f"{CURRENT_EXPR}{part}"
    else:
        CURRENT_EXPR = part

    return {"message": "добавлено выражение", "current_expression": CURRENT_EXPR}

@app.get("/calc_expr_str/{expr}")
async def calc_expr_str(expr: str):
    try:
        result = eval(expr)
        return {"expression": expr, "result": result}
    except Exception as e:
        return {"error": f"Ошибка вычисления: {e}"}

@app.get("/get_expr")
async def get_expr():
    return {"current_expression": CURRENT_EXPR or "не задано"}

@app.get("/calc_expr")
async def calc_expr():
    global CURRENT_EXPR
    if not CURRENT_EXPR:
        return {"error": "сначала создайте выражение"}
    try:
        result = eval(CURRENT_EXPR)
        return {"expression": CURRENT_EXPR, "result": result}
    except Exception as e:
        return {"error": f"ошибка вычисления: {e}"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )