#!/bin/bash

# EC2 Setup Script for Avianis ETL
# This script sets up the Python virtual environment, installs dependencies, and prepares the environment

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== EC2 Setup for Avianis ETL ===${NC}"

# Get script directory and change to it
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if we're in the right directory
if [ ! -f "etl_pipeline.py" ]; then
    echo -e "${RED}ERROR: etl_pipeline.py not found. Please run this script from the avianis-etl directory.${NC}"
    exit 1
fi

# Check Python version
echo -e "${BLUE}Checking Python installation...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 is not installed.${NC}"
    echo "Please install Python 3 first:"
    echo "  sudo yum update -y"
    echo "  sudo yum install -y python3 python3-pip python3-venv"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}✓ Python $PYTHON_VERSION found${NC}"

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo -e "${YELLOW}Installing pip...${NC}"
    sudo yum install -y python3-pip
fi

# Install virtual environment if not available
if ! python3 -m venv --help &> /dev/null; then
    echo -e "${YELLOW}Installing python3-venv...${NC}"
    sudo yum install -y python3-venv
fi

# Remove existing virtual environment if it exists
if [ -d "venv" ]; then
    echo -e "${YELLOW}Removing existing virtual environment...${NC}"
    rm -rf venv
fi

# Create virtual environment
echo -e "${BLUE}Creating virtual environment...${NC}"
python3 -m venv venv

# Activate virtual environment
echo -e "${BLUE}Activating virtual environment...${NC}"
source venv/bin/activate

# Upgrade pip
echo -e "${BLUE}Upgrading pip...${NC}"
pip install --upgrade pip

# Install dependencies
echo -e "${BLUE}Installing dependencies from requirements.txt...${NC}"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed successfully${NC}"
else
    echo -e "${RED}ERROR: requirements.txt not found${NC}"
    exit 1
fi

# Create logs directory structure
echo -e "${BLUE}Creating log directories...${NC}"
mkdir -p logs/test
mkdir -p logs/jetaccess

# Make scripts executable
echo -e "${BLUE}Making scripts executable...${NC}"
chmod +x run_etl.sh

# Verify installation
echo -e "${BLUE}Verifying installation...${NC}"
python -c "import pandas, sqlalchemy, pymysql, requests, schedule; print('✓ All required packages imported successfully')" && echo -e "${GREEN}✓ Installation verified${NC}" || echo -e "${RED}✗ Installation verification failed${NC}"

echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Set up your database connection in .env file"
echo "2. Configure your Avianis API credentials in .env file"
echo ""
echo -e "${BLUE}To run the ETL pipeline:${NC}"
echo "  ./run_etl.sh jetaccess full"
echo ""
echo -e "${BLUE}To activate the virtual environment manually:${NC}"
echo "  source venv/bin/activate"
echo ""
echo -e "${BLUE}To deactivate the virtual environment:${NC}"
echo "  deactivate"