# Cloud Stream Worker

A lightweight asynchronous stream processing and WebSocket telemetry microservice built with Python, FastAPI, and Uvicorn.

## Features
- High-performance asynchronous WebSocket streaming pipeline
- Real-time telemetry endpoints and JSON metrics
- Containerized microservice design with Python 3.13-slim
- Built-in connection lifecycle management and health monitoring

## Architecture
- **Framework:** FastAPI
- **ASGI Server:** Uvicorn
- **Default Port:** 8080
- **Container Base:** python:3.13-slim

## Local Development
```bash
python -m venv .venv
source .venv/bin/activate  # Or .venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080
```

## Health Check
```bash
curl http://localhost:8080/healthz
```
