#!/usr/bin/env bash

# Check whether dev requirements are already initialized.
if [ ! -f "./requirements-dev.txt" ]; then
    echo "Installing dev dependencies..."
    sed '/-e/d' requirements-dev.lock > requirements-dev.txt
    pip install -r requirements-dev.txt --user
    echo "Dev dependencies have been installed successfully!"
fi

sleep infinity
