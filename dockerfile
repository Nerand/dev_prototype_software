FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV DB_URL=sqlite:///students_simple.db
ENV REDIS_URL=redis://redis:6379/0
EXPOSE 8000
CMD ["uvicorn", "end_homework_for_2ppa:app", "--host", "0.0.0.0", "--port", "8000"]
