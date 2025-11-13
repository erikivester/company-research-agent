# Stage 1: Build Backend (Where compilation happens)
FROM python:3.11-slim AS backend-builder
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install uv
RUN uv pip install -r requirements.txt --system

# Stage 2: Final Image (CRITICAL RUNTIME FIX)
FROM python:3.11-slim
WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create the non-root user FIRST
RUN useradd -m -u 1000 appuser
RUN mkdir -p /app/pdfs /secrets
# Set permissions on the app directory
RUN chown -R appuser:appuser /app

# Copy python packages from the build stage
COPY --from=backend-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

# Copy application files
COPY backend/ ./backend/
COPY application.py .
COPY requirements.txt . 

# --- NEW FIX: Force-delete all __pycache__ directories ---
RUN find . \
    -type d -name "__pycache__" -exec rm -r {} +

# --- OPTIONAL: Copy GDrive credentials if building locally ---
# In production, this will be mounted from Secret Manager
# COPY gdrive_credentials.json /secrets/gdrive_credentials.json

# Change ownership of all code
RUN chown -R appuser:appuser /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

EXPOSE 8000 8001

# Switch to the non-root user AT THE VERY END
USER appuser

# Start the application via application.py to also start metrics on 8001
CMD ["python", "application.py"]