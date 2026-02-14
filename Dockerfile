# Start with slim Python 3.13 image
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

# Set working directory
WORKDIR /opt/pipelines

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/opt/pipelines/.venv/bin:$PATH"

# Copy dependency files
COPY ./pyproject.toml ./.python-version ./uv.lock /opt/pipelines

# Copy application code
COPY ./ingest_data.py /opt/pipelines/

# Install dependencies from lock file
RUN uv sync --locked

# Execute the data ingestion script
ENTRYPOINT ["python", "ingest_data.py"]