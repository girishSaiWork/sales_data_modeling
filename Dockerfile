FROM python:3.8-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install JupyterLab
RUN pip install jupyterlab

# Copy the rest of your app
COPY . .

# Expose port for Jupyter
EXPOSE 8888

# Start JupyterLab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8889", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]
