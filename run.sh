#!/bin/bash
set -e

# Load .env if exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Install deps if needed
if [ ! -d "venv" ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip install -r app/requirements.txt
else
    source venv/bin/activate
fi

# Run app
cd app
uvicorn main:app --reload --host 0.0.0.0 --port 8000
