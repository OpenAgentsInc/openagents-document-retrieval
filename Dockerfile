FROM python:3.10-slim
ADD src /app
ADD requirements.txt /app
WORKDIR /app
RUN pip install -r requirements.txt

ENV POOL_ADDRESS="127.0.0.1"
ENV POOL_PORT="5000"
ENV CACHE_PATH="/cache"
ENV MAX_SIMULTANEOUS_REQUESTS=4
ENV MAX_WORKERS=8
VOLUME /cache

CMD ["python", "-u", "main.py"]
