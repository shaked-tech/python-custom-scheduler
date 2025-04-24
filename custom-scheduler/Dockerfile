# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY src/requirements.txt .

# Install any needed packages specified in requirements.txt
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the source code into the container at /app/src
COPY src/ ./src/

# Set environment variables (optional, can be overridden at runtime)
# Example: ENV LOG_LEVEL=INFO

# Define the command to run your application
CMD ["python", "src/main.py"]
