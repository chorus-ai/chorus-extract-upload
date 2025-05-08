FROM python:3.12-slim

RUN mkdir /app 
# && mkdir /cache && mkdir /mnt
WORKDIR /app


RUN apt-get update && \
    apt-get install -y \
    git \
    curl \
    libfuse3-dev fuse3 \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsS https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb > /app/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb
RUN apt-get update && apt-get install -y\
    blobfuse2 \
    && rm -rf /var/lib/apt/lists/*
# if building blobfuse2 directly, would need to install
# make cmake gcc g++ parallel \
# also, golang package 1.23 needed so have to install directly.


# # install go
# RUN curl -LsS https://go.dev/dl/go1.23.4.linux-amd64.tar.gz > /app/go1.23.4.linux-amd64.tar.gz  && \
#     rm -rf /usr/local/go && \
#     tar -C /usr/local -xzf /app/go1.23.4.linux-amd64.tar.gz

# ENV PATH=$PATH:/usr/local/go/bin

# install blobfuse2
# # safe.directory to avoid VCS stamp problem.
# RUN git clone https://github.com/Azure/azure-storage-fuse/
# WORKDIR /app/azure-storage-fuse
# RUN git checkout main && \
#     git config --global --add safe.directory /app/azure-storage-fuse && \
#     go get && \
#     go build -tags=fuse3


# configure fuse2

# install azcli
RUN curl -LsS https://aka.ms/InstallAzureCLIDeb | bash && rm -rf /var/lib/apt/lists/*

# copy in the source code
RUN git clone https://github.com/chorus-ai/chorus-extract-upload.git 
WORKDIR /app/chorus-extract-upload
ENV FLIT_ROOT_INSTALL=1

# install chorus uploader
RUN pip install --no-cache-dir flit && \
    git config --global --add safe.directory /app/chorus-extract-upload && \
    flit install --symlink

# copy in the config.toml file
# COPY /app/chorus-extract-upload/chorus_upload/config.toml.template /app/chorus-extract-upload/config.toml

# entrypoint in "exec" form
ENV AZURE_CLI_DISABLE_CONNECTION_VERIFICATION 1

# Can't mount here without saving the sas token.  has to be invoked from console.
# RUN blobfuse2 mount all /mnt --config-file=/app/chorus-extract-upload/configurations/blobfuse2.yaml

# CMD ["-c", "/app/chorus-extract-upload/config.toml", "journal", "update"]
# ENTRYPOINT [ "python", "chorus_upload" ]

ENTRYPOINT ["/bin/bash"]
