# Use official Python image
FROM python:3.10

# Set working directory
WORKDIR /app

# Install uv globally
RUN pip install uv

# Create and activate virtual environment
RUN uv venv .venv

# Copy dependency file
COPY pyproject.toml ./

# Install dependencies inside the virtual environment
RUN uv pip install

# Copy all project files
COPY . .

# Set entry point for FastAPI with activated virtual environment
CMD ["sh", "-c", ".venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --reload"]

