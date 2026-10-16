# Lightweight multi-arch container for biosignal cloud receiver & processor
FROM python:3.11-alpine

WORKDIR /app

# Copy application files
COPY common/ common/
COPY cloud/ cloud/

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

CMD ["python", "-m", "cloud.server", "--port", "8080"]
