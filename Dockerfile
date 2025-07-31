FROM astrocrpublic.azurecr.io/runtime:3.0-6


USER astro

# install dbt into a virtual environment
RUN python -m venv .venv && source .venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate

# install mc
RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

RUN chmod +x $HOME/minio-binaries/mc
RUN echo "export PATH=$PATH:$HOME/minio-binaries/" >> ~/.bashrc   
RUN mc alias set myminio https://10.90.65.61 selabd MinIO2008*

RUN mkdir -p /tmp/data
