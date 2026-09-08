FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev && rm -rf /var/lib/apt/lists/*

# Install standard requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install streamlit and playwright python client (though included in base, good to be explicit)
RUN pip install --no-cache-dir streamlit playwright

# Optional: Ensure playwright browsers are fully installed (base image usually has them, but safe measure)
RUN playwright install chromium

# Expose Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "apps/frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
