# Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /scripts

# Copy dependencies and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scripts
COPY . .

# Set entrypoint to Python
ENTRYPOINT ["python"]

# Default command (can be overridden by Airflow)
CMD ["extract.py"]


#IMAGE PUSHE TO DOCKER HUB
#NAME:  zainabojo/travel-agency-pipeline-image
#TAG :CDE 