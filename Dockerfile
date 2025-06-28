FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir pathway pandas

CMD ["python", "pathway_pipeline.py"]
