# syntax=docker/dockerfile:1
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY faker_generator.py .
RUN useradd --system --uid 10001 --no-create-home app
USER app

CMD ["python", "faker_generator.py"]
