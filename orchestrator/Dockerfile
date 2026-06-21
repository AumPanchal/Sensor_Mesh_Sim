FROM python:3.12-alpine
WORKDIR /app
RUN pip install docker
COPY orchestrator.py .
COPY index.html .
CMD ["python", "orchestrator.py"]