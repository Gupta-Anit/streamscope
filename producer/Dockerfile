# Slim Python base — small image, fast pull
FROM python:3.12-slim

WORKDIR /app

# Install deps first for better Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script
COPY producer.py .

# Run as non-root user (security best practice)
RUN useradd -m -u 1000 producer && chown -R producer:producer /app
USER producer

# -u flag = unbuffered stdout so docker logs show output in real time
CMD ["python", "-u", "producer.py"]
