FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir fastapi uvicorn

COPY . .

ENV PORT=8000

EXPOSE ${PORT}

CMD uvicorn server:app --host 0.0.0.0 --port ${PORT}
