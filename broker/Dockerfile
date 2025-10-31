FROM python:3.9-slim
WORKDIR /app
RUN pip install pyzmq msgpack
COPY broker.py .
CMD ["python", "-u", "broker.py"]