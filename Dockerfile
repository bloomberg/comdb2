#Use the official Debian-based image
FROM debian:bullseye

# Set the non-interactive mode for apt environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites
RUN apt-get update && apt-get install -y \
    bison \
    build-essential \
    cmake \
    flex \
    libevent-dev \
    liblz4-dev \
    libprotobuf-c-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssl-dev \
    libunwind-dev \
    ncurses-dev \
    protobuf-c-compiler \
    tcl \
    uuid-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Add /opt/bb/bin to PATH
ENV PATH="${PATH}:/opt/bb/bin"

# Set the working directory
WORKDIR /app

# Copy the project source code to the container
COPY . /app

# Build and install Comdb2
RUN mkdir build && cd build && cmake .. && make && make install

# Start pmux
CMD ["pmux", "-n"]

# Expose any necessary ports
EXPOSE 80