FROM python:3.11-slim

# Install build essentials and CMake
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        python3-dev \
        git \
        pybind11-dev && \
    rm -rf /var/lib/apt/lists/*

# Install pybind11
RUN pip install pybind11

WORKDIR /app

# Copy source code
COPY . /app

CMD ["bash"] 