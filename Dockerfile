FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# copy in the source code
RUN mkdir /app
WORKDIR /app
RUN git clone https://github.com/chorus-ai/chorus-extract-upload.git 
WORKDIR /app/chorus-extract-upload

# install azcli
RUN curl -LsS https://aka.ms/InstallAzureCLIDeb | bash && rm -rf /var/lib/apt/lists/*

# install
RUN pip install --no-cache-dir flit
ENV FLIT_ROOT_INSTALL=1
RUN flit install --symlink

# copy in the config.toml file
COPY config.toml /app/chorus-extract-upload/config.toml

# entrypoint in "exec" form
ENV AZURE
CMD ["-c", "/app/chorus-extract-upload/config.toml", "journal", "update"]
ENTRYPOINT [ "python", "chorus_upload" ]
