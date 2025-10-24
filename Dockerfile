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
# FIX: Install core build tools required by many Python packages (e.g., reportlab)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    # Add libpq-dev if you later connect to Postgres, etc.
    && rm -rf /var/lib/apt/lists/*

# 🟢 NEW: Install uv using pip
RUN pip install uv

# 🟢 FIX: Install all Python dependencies using uv --system
# This installs dependencies globally within the Docker container.
RUN uv pip install -r requirements.txt --system

# Stage 3: Final Image (CRITICAL RUNTIME FIX)
FROM python:3.11-slim
WORKDIR /app

# FIX: Install runtime dependencies for stability and PDF generation
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    # CRITICAL: Install necessary fonts for reportlab to function correctly at runtime
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

# FIX: This command correctly starts the Uvicorn server on the port provided by Cloud Run.
CMD python -m uvicorn application:app --host 0.0.0.0 --port $PORT