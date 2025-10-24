# Stage 1: Build Frontend
FROM node:20-slim AS frontend-builder
WORKDIR /app/ui
COPY ui/package*.json ./
# Use npm ci for clean installs in automated environments
RUN npm ci
COPY ui/ ./
RUN npm run build

# Stage 2: Build Backend (Where compilation happens)
FROM python:3.11-slim AS backend-builder
WORKDIR /app
COPY requirements.txt .

# CRITICAL FIX: Install core build tools and headers required for Python packages (e.g., reportlab, Pillow)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install all Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 3: Final Image (CRITICAL RUNTIME FIX)
FROM python:3.11-slim
WORKDIR /app

# CRITICAL FIX: Install minimal runtime system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    # Runtime libraries for image/PDF processing (Pillow, reportlab)
    libjpeg62-turbo \
    zlib1g \
    # Runtime fonts required by reportlab to prevent crashes
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

# Corrected CMD (Uses Python's Module System and environment variable)
CMD ["python", "-m", "uvicorn", "application:app", "--host", "0.0.0.0", "--port", "${PORT}"]