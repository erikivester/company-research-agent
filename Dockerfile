# Stage 1: Build Frontend
FROM node:20-slim AS frontend-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# Stage 2: Build Backend (Where compilation happens)
FROM python:3.11-slim AS backend-builder
WORKDIR /app
COPY requirements.txt .

# CRITICAL FIX (Stage 2): Install core build tools and headers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    libfreetype6-dev \
    && rm -rf /var/lib/apt/lists/*

# Install all Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 3: Final Image (Runtime)
FROM python:3.11-slim
WORKDIR /app

# CRITICAL FIX (Stage 3): Install minimal runtime system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libjpeg62-turbo \
    zlib1g \
    libpng16-16 \
    libfreetype6 \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Copy python packages from the build stage
COPY --from=backend-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

# Copy application files
COPY backend/ ./backend/
COPY application.py .
COPY requirements.txt . 

# Copy frontend build 
COPY --from=frontend-builder /app/ui/dist/ ./ui/dist/

# Create reports/PDFs directory 
RUN mkdir -p pdfs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080 

EXPOSE 8080

# Create a non-root user (Good security practice)
RUN useradd -m -u 1000 appuser
RUN chown -R appuser:appuser /app
USER appuser

# FINAL CORRECTED STARTUP COMMAND
CMD ["python", "-m", "uvicorn", "application:app", "--host", "0.0.0.0", "--port", "${PORT}"]