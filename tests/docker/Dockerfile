# BenchFS Docker Image with MPI Support
# Multi-stage build for optimized image size

FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    pkg-config \
    libssl-dev \
    cmake \
    autoconf \
    automake \
    libtool \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install UCX (Unified Communication X)
WORKDIR /tmp
RUN wget https://github.com/openucx/ucx/releases/download/v1.18.0/ucx-1.18.0.tar.gz && \
    tar xzf ucx-1.18.0.tar.gz && \
    cd ucx-1.18.0 && \
    ./configure --prefix=/usr/local \
                --enable-mt \
                --disable-numa \
                --without-verbs \
                --without-rocm \
                --without-cuda && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf ucx-1.15.0*

# Install OpenMPI
RUN wget https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-4.1.5.tar.gz && \
    tar xzf openmpi-4.1.5.tar.gz && \
    cd openmpi-4.1.5 && \
    ./configure --prefix=/usr/local \
                --with-ucx=/usr/local \
                --enable-mpi-cxx && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf openmpi-4.1.5*

# Copy pluvio runtime (required dependency)
WORKDIR /build
COPY pluvio /build/pluvio

# Copy BenchFS source
COPY Cargo.toml Cargo.lock /build/
COPY src /build/src/
COPY build.rs /build/

# Build BenchFS with MPI support
RUN cargo build --release --features mpi-support

# Runtime stage
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    openssh-server \
    && rm -rf /var/lib/apt/lists/*

# Copy UCX and MPI from builder
COPY --from=builder /usr/local /usr/local
RUN ldconfig

# Copy BenchFS binaries
COPY --from=builder /build/target/release/benchfsd /usr/local/bin/
COPY --from=builder /build/target/release/benchfsd_mpi /usr/local/bin/

# Setup SSH for MPI
RUN mkdir /var/run/sshd && \
    ssh-keygen -A && \
    mkdir -p /root/.ssh && \
    ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile=/dev/null" >> /etc/ssh/ssh_config

# Create directories
RUN mkdir -p /shared/registry /shared/data /logs

# Environment variables
ENV UCX_TLS=tcp,sm,self
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

WORKDIR /workspace

# Expose SSH port
EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
