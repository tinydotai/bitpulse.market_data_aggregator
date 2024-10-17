# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY src /app/src
COPY .env /app/.env
COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the Python path to include the src directory
ENV PYTHONPATH=/app/src

# Run get_binance_transactions.py when the container launches
CMD ["python", "/app/src/get_coingecko_data.py"]