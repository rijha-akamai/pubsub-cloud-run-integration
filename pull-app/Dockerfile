# Use the official Python image
FROM python:3.9-slim
# Set the working directory inside the container
WORKDIR /app
# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy the application code into the container
COPY app.py .
# Expose the application port (Cloud Run uses 8080 by default)
EXPOSE 8080
# Run the Flask app when the container starts
CMD ["python", "app.py"]
