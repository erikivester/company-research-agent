# Stage 2: Build Backend (Where compilation happens)
FROM python:3.11-slim AS backend-builder
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install uv
RUN uv pip install -r requirements.txt --system

# Stage 3: Final Image (CRITICAL RUNTIME FIX)
FROM python:3.11-slim
WORKDIR /app

# --- START OF CHANGES ---

# Create the non-root user FIRST
RUN useradd -m -u 1000 appuser
RUN mkdir -p /app/pdfs /secrets
# Set permissions on the directories BEFORE copying
RUN chown -R appuser:appuser /app

# Copy python packages from the build stage
COPY --from=backend-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

# Copy application files AS ROOT first
COPY backend/ ./backend/
COPY application.py .
COPY requirements.txt . 

# NOW, change ownership of the copied code to the appuser
RUN chown -R appuser:appuser /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080 
ENV PYTHONPATH /app

EXPOSE 8080

# Switch to the non-root user AT THE VERY END
USER appuser

# Start the application
CMD python -m uvicorn application:app --host 0.0.0.0 --port $PORT

# --- END OF CHANGES ---# Stage 2: Build Backend (Where compilation happens)
FROM python:3.11-slim AS backend-builder
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install uv
RUN uv pip install -r requirements.txt --system

# Stage 3: Final Image (CRITICAL RUNTIME FIX)
FROM python:3.11-slim
WORKDIR /app

# --- START OF CHANGES ---

# Create the non-root user FIRST
RUN useradd -m -u 1000 appuser
RUN mkdir -p /app/pdfs /secrets
# Set permissions on the directories BEFORE copying
RUN chown -R appuser:appuser /app

# Copy python packages from the build stage
COPY --from=backend-builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

# Copy application files AS ROOT first
COPY backend/ ./backend/
COPY application.py .
COPY requirements.txt . 

# NOW, change ownership of the copied code to the appuser
RUN chown -R appuser:appuser /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080 
ENV PYTHONPATH /app

EXPOSE 8080

# Switch to the non-root user AT THE VERY END
USER appuser

# Start the application
CMD python -m uvicorn application:app --host 0.0.0.0 --port $PORT

# --- END OF CHANGES ---