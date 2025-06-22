# Boat Sales ETL Pipeline (Pandas + Pandera + PySpark)

This project performs ETL (Extract, Transform, Load) on a CSV dataset of boat sales. It uses **Pandas** and **Pandera** for cleaning and validation, and **PySpark** for distributed processing and Parquet output.

## Features

- Cleans and transforms real-world messy CSV data
- Unicode and encoding handling
- Country/location normalization (with robust mapping)
- Schema validation using `pandera`
- Spark-based aggregation and Parquet output
- CLI with `argparse`
- Works locally without Databricks


## Conda Environment Setup

### 1. Install Conda (if needed)

https://docs.conda.io/en/latest/miniconda.html

### 2. Create and activate a virtual environment

```bash
conda create -n myenv python=3.11 -y
conda activate myenv
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Install Java 11 (Spark-compatible)

```bash
brew install openjdk@11
```

### 5. Set Java for the Conda env

```bash
mkdir -p $(conda info --base)/envs/myenv/etc/conda/activate.d
mkdir -p $(conda info --base)/envs/myenv/etc/conda/deactivate.d

echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' > $(conda info --base)/envs/myenv/etc/conda/activate.d/env_vars.sh
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> $(conda info --base)/envs/myenv/etc/conda/activate.d/env_vars.sh
echo 'unset JAVA_HOME' > $(conda info --base)/envs/myenv/etc/conda/deactivate.d/env_vars.sh

conda deactivate
conda activate myenv
```

### 6. Verify Java and PySpark

```bash
java -version     # Should show Java 11
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Run the ETL Pipeline

```bash
cd test
./test.sh
```

- Cleans and validates the CSV file using Pandas + Pandera
- Writes output to `output/data.parquet`
- Creates summary CSV as `output/data_summary.csv`


## Usage

You can run the ETL pipeline as follows.

```bash
  boat-etl \
  -i data/boat_data.csv \
  -o output/data.parquet \
  -s output/data_summary.csv \
```

## Docker Usage

You can run the ETL pipeline inside a Docker container for reproducibility and ease of deployment.

## Run with Docker

### 1. Build the Docker image

```bash
docker build -t boat-etl .
```

### 2. Run the container


Run the container by specifying the  input and output paths:


```bash
docker run --rm \
  -v "$PWD/data":/app/data \
  -v "$PWD/output":/app/output \
  boat-etl \
  python src/pipeline_pyspark.py -i data/boat_data.csv -o output/data.parquet -s output/data_summary.csv
```

## Running on Azure Databricks

To run this ETL pipeline on Azure Databricks:

1. Go to your [Databricks workspace](https://adb-*.azuredatabricks.net) in Azure.

2. In the left sidebar, select **Workspace > Users > your-user-name**.

3. Click the drop-down next to your username and choose **Import**.

4. Select **File**, then upload the script located at `databricks/boat_etl_databricks.py`. This will create a Python notebook you can run.

5. Next, upload your input CSV file:
   - Go to **Data > DBFS > FileStore**
   - Upload your file to: `/FileStore/data/boat_data.csv`

6. Open the uploaded notebook and verify that the file paths are set like this:

   ```python
   input_path = "/dbfs/FileStore/data/boat_data.csv"
   parquet_output_path = "/dbfs/FileStore/output/data.parquet"
   summary_output_path = "/dbfs/FileStore/output/data_summary.csv"
   ```

7. Click **Run All** in the notebook to execute the ETL pipeline. This will:

- Clean and validate the CSV file
- Save the output as Parquet
- Generate a summary CSV grouped by country

8. To download the outputs, open these URLs in your browser (replacing `<your-workspace-url>` with your Databricks workspace):

- `https://<your-workspace-url>/files/output/data.parquet`
- `https://<your-workspace-url>/files/output/data_summary.csv`


