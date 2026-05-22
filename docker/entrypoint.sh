#!/usr/bin/env bash

# Check whether dev requirements are already initialized.
echo "Installing dev dependencies..."
uv sync --dev
echo "Dev dependencies have been installed successfully!"

sleep infinity
