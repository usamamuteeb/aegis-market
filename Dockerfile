FROM python:3.14-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY aegis ./aegis
COPY worker.py spot-study-summary.json ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "worker.py"]
