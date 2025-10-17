FROM python:3.9-slim

WORKDIR /app

RUN pip install pyzmq

COPY server.py .

CMD ["python", "-u", "server.py"]