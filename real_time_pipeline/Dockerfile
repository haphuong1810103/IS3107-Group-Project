# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy your script into the container
COPY real_time_streaming.py /app/

# Install dependencies
RUN pip install --no-cache-dir yfinance pandas google-cloud-pubsub

# Run the script when the container starts
CMD ["python3", "real_time_streaming.py"]