# Environment Setup Guide

## Quick Start

This project requires **Python 3.11.x** - the specific dependencies have been configured to use pre-compiled binary wheels to avoid compilation issues.

### Step 1: Install Python 3.11.9

#### Windows
1. Download Python 3.11.9 installer from [python.org](https://www.python.org/downloads/release/python-3119/)
2. Run the installer, select "Add Python to PATH"
3. Verify installation: `python --version`

#### Using pyenv (Cross-platform)
```bash
pyenv install 3.11.9
pyenv local 3.11.9  # Sets project directory to use 3.11.9
```

### Step 2: Create a Fresh Virtual Environment

#### Windows
```powershell
# Remove existing environment if present
if (Test-Path -Path ".\compendium-env") {
    Remove-Item -Path ".\compendium-env" -Recurse -Force
}

# Create new environment
python -m venv compendium-env

# Activate
.\compendium-env\Scripts\activate
```

#### macOS/Linux
```bash
# Remove existing environment if present
rm -rf compendium-env

# Create new environment
python -m venv compendium-env

# Activate
source compendium-env/bin/activate
```

### Step 3: Install Dependencies

```bash
# Update pip, setuptools, and wheel
python -m pip install --upgrade pip setuptools wheel

# Install dependencies with binary wheels
pip install -r requirements.txt

# Verify no broken dependencies
pip check
```

### Step 4: Verify Installation

Run the smoke test to verify that all collector modules can be imported:

```bash
python smoke_test_collectors.py
```

### Common Issues & Solutions

1. **Meson build errors**: These indicate you're using a Python version without pre-compiled wheels. Double-check that you're using Python 3.11.x.

2. **ImportError: lxml module**: 
   - Ensure you're in the Python 3.11 environment before installing
   - Verify with `pip list | grep lxml`

3. **ValueError: Unsupported httpx version**:
   - The requirements.txt specifically pins httpx==0.23.3 for compatibility with pyalex

4. **Google Scholar 429 errors**:
   - These are normal and indicate rate limiting
   - Use ScholarCollector last, increase sleep_seconds parameter, or use a proxy

5. **spacy.exc.Errors.E050 about model**:
   - Run `python -m spacy download en_core_web_sm`

## For Deployment/CI

If deploying or using continuous integration, ensure your environment uses Python 3.11.x by:
- Setting the Python version in your CI config
- Using the Docker image `python:3.11.9` if containerizing
- Including the .python-version file in your repo
