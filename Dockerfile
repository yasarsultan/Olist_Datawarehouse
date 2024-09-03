# Use the latest minimal Python image from the Docker Hub
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt file to leverage Docker cache
COPY requirements.txt .

# Install python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy current directory contents into the container at /app
COPY . .

# Command to run the data pipeline
CMD ["python", "datapipeline.py"]