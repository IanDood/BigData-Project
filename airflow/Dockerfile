FROM apache/airflow:2.10.5

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    software-properties-common \
    gcc \
    python3-dev \
    libpq-dev && \
    apt-get clean

# Download and extract OpenJDK 11 (Adoptium)
RUN wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.13%2B8/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.13_8.tar.gz && \
    tar -xvzf OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.13_8.tar.gz -C /opt && \
    rm OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.13_8.tar.gz

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/opt/jdk-11.0.13+8
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to airflow user for pip installs
USER airflow

# Install Python dependencies
RUN pip install \
    pandas==2.1.2 \
    psycopg2-binary \
    beautifulsoup4 \
    apache-airflow-providers-postgres==6.0.0 \
    apache-airflow-providers-apache-spark==2.0.1 \
    nba_api \
    requests



