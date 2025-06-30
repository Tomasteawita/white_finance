FROM jupyter/base-notebook:x86_64-python-3.11.6

# Set the working directory
WORKDIR /home/jovyan/work

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port for Jupyter
EXPOSE 8888

# Command to start Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.allow_origin='*'"]