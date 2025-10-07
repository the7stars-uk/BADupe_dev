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

# STEP 3: Bring in the list of ingredients first.
# We copy ONLY the shopping list into the kitchen.
COPY requirements.txt .

# STEP 4: Install the ingredients.
# The robot chef goes to the store and installs these exact ingredients
# onto the kitchen shelves. This is done during the build.
RUN pip install --no-cache-dir -r requirements.txt

# STEP 5: Bring in the recipe itself (your code).
# Now that the kitchen is prepped, we copy your Python scripts in.
COPY . .

# STEP 6: Specify what to do when the "ON" button is pressed.
# This is the final instruction. When the kitchen is activated (a container starts),
# the robot chef should immediately start the production web server.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app