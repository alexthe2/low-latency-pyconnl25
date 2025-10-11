#!/usr/bin/env bash
set -e

echo "🔧 Setting up virtual environment..."

# choose python 3.13 if installed, else fallback
PYTHON=$(command -v python3.13 || command -v python3)
VENV_DIR=".venv"

# create venv
$PYTHON -m venv "$VENV_DIR"

echo
echo "✅ Virtual environment created at $VENV_DIR"
echo "👉 Activate it with:"
echo "   source $VENV_DIR/bin/activate"
