FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y curl rsync && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN apt-get update && apt-get install -y \
    build-essential \
    uuid-dev \
    libgpgme-dev \
    squashfs-tools \
    libseccomp-dev \
    wget \
    pkg-config \
    git \
    docker.io \
    cryptsetup-bin

RUN wget https://dl.google.com/go/go1.14.12.linux-amd64.tar.gz && \
    tar -C /usr/local -xzvf go1.14.12.linux-amd64.tar.gz && \
    rm go1.14.12.linux-amd64.tar.gz

RUN echo 'export GOPATH=${HOME}/go' >> ~/.bashrc && \
    echo 'export PATH=/usr/local/go/bin:${PATH}:${GOPATH}/bin' >> ~/.bashrc

RUN git clone https://github.com/hpcng/singularity.git && \
    cd singularity && \
    git checkout v3.7.0

# The bashrc must be reloaded so that singularity scripts don't fail because "go" isn't in path.
RUN bash -c "source ~/.bashrc && cd singularity && ./mconfig && make -C ./builddir && make -C ./builddir install"
ENV PATH="/root/.local/bin:$PATH"

RUN pip install uvicorn
RUN mkdir -p /data/sched_storage

COPY ../ .
# All the .env variables will be reassigned as env vars in docker compose.
RUN rm .env
RUN poetry install
