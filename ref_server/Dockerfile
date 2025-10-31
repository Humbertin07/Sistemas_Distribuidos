FROM python:3.9-slim
WORKDIR /app
RUN pip install pyzmq msgpack
COPY ref_server.py .
CMD ["python", "-u", "ref_server.py"]