FROM python:3.9-slim
WORKDIR /app
RUN pip install pyzmq msgpack
COPY bot.py .
CMD ["python", "-u", "bot.py"]