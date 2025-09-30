# Dockerfile

# STEP 1: Start with a pre-built foundation.
# We are not building a kitchen from raw metal. We're starting with a
# standard model that already has electricity and plumbing.
FROM python:3.10-slim

# STEP 2: Set up the internal workspace.
# We're telling the robot chef that all future work should happen
# in a specific area labeled "/app".
ENV APP_HOME /app
WORKDIR $APP_HOME

# <<< --- STEP 3: Install system dependencies needed for building packages --- >>>
# This is the key fix. 'build-essential' installs the C compiler and other
# tools that packages like numpy need to install correctly.
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# STEP 4: Bring in the list of ingredients first.
# We copy ONLY the shopping list into the kitchen.
COPY requirements.txt .

# STEP 5: Install the ingredients.
# The robot chef goes to the store and installs these exact ingredients
# onto the kitchen shelves. This is done during the build.
RUN pip install --no-cache-dir -r requirements.txt

# STEP 6: Bring in the recipe itself (your code).
# Now that the kitchen is prepped, we copy your Python scripts in.
COPY . .

# STEP 7: Specify what to do when the "ON" button is pressed.
# This is the final instruction. When the kitchen is activated (a container starts),
# the robot chef should immediately start the production web server.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app