FROM ubuntu:24.04

# Set the working directory
WORKDIR /app
# copy uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# Copy the project files
COPY crawler.py .
COPY pyproject.toml .
COPY uv.lock .

# Use uv to sync dependencies (pyproject.toml and uv.lock are assumed to be present)
RUN uv sync

# Run the crawler script using uv
CMD ["uv", "run", "python", "/app/crawler.py"]
