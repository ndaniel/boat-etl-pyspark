# Use an official Python image
FROM python:3.11

# Install Java 11 (required by PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jdk wget git && apt-get clean

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set workdir
WORKDIR /app
RUN mkdir -p /app/output

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Default command
CMD ["python", "src/pipeline_pyspark.py", "-i", "data/boat_data.csv", "-o", "output/data.parquet", "-s", "output/data_summary.csv"]

