FROM python:3.9-slim

WORKDIR /app

RUN pip install pyzmq msgpack

COPY server.py .

CMD ["python", "-u", "server.py"]