FROM quay.io/astronomer/astro-runtime:12.6.0

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Airflow settings
COPY airflow_settings.yaml .