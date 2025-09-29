# Avianis ETL Deployment Guide

## Quick Start for EC2

### 1. Environment Setup (One-time)
```bash
# Run this once to set up Python environment and dependencies
./setup_ec2.sh
```

This will:
- Create Python virtual environment
- Install all required packages from requirements.txt
- Set up log directories
- Verify installation

### 2. Database Configuration
Create a `.env` file with your database and API credentials:
```bash
cp .env.example .env
# Edit .env with your actual values
```

### 3. Initial Data Setup (One-time per operator)
```bash
# Load reference data: aircraft types, categories, aircraft, and crew
./run_etl.sh jetaccess setup
```

This populates the foundational tables that don't change frequently:
- Aircraft categories
- Aircraft types/models  
- Aircraft records
- Crew/personnel records

### 4. Regular ETL Operations
```bash
# Run complete ETL pipeline (movements, demand, crew assignments)
./run_etl.sh jetaccess full

# Or run specific components:
./run_etl.sh jetaccess flight-data    # Flight legs and crew assignments only
./run_etl.sh jetaccess crew           # Update crew data only
./run_etl.sh jetaccess aircraft       # Update aircraft data only
```

## Command Reference

### Environment Setup
- `./setup_ec2.sh` - Set up Python environment and dependencies (run once)

### Data Operations  
- `./run_etl.sh [operator] setup` - Initial reference data setup (run once per operator)
- `./run_etl.sh [operator] full` - Complete ETL pipeline (regular use)
- `./run_etl.sh [operator] flight-data` - Flight movements and crew assignments only
- `./run_etl.sh [operator] crew` - Crew data only
- `./run_etl.sh [operator] aircraft` - Aircraft data only

### Supported Operators
- `test` (default)
- `jetaccess`

## Typical Deployment Workflow

```bash
# 1. First time setup
./setup_ec2.sh

# 2. Configure credentials
vim .env

# 3. Initial data load (run once)
./run_etl.sh jetaccess setup

# 4. Regular ETL runs (daily/hourly)
./run_etl.sh jetaccess full
```

## Logs

Logs are automatically created in `logs/{operator}/` directory with timestamps.

## Troubleshooting

**"No module named 'pandas'"**
- Run `./setup_ec2.sh` to set up the virtual environment

**Virtual environment not found**  
- Run `./setup_ec2.sh` to create it

**Database connection errors**
- Check your `.env` file configuration
- Verify database server is accessible