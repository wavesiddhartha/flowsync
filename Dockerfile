FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy packages
COPY packages/flowsync-core /app/packages/flowsync-core

# Install package and dependencies
RUN pip install --no-cache-dir -e "/app/packages/flowsync-core"

# Create a non-root user and set permissions
RUN useradd -m -u 10011 flowsync && chown -R flowsync:flowsync /app
USER flowsync

EXPOSE 8765

CMD ["flowsync", "dev", "--host", "0.0.0.0", "--port", "8765"]
