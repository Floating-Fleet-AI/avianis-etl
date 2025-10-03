#!/bin/bash

# Avianis ETL Pipeline Runner Script
# This script provides convenient commands to run various ETL operations with operator support

set -e  # Exit on any error

# Get script directory and change to it
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if operator parameter is provided, default to "test"
if [ $# -eq 0 ]; then
    OPERATOR="test"
    echo -e "${YELLOW}No operator specified, defaulting to 'test'${NC}"
else
    OPERATOR="$1"
    shift  # Remove operator from arguments
fi

# Validate operator
case "$OPERATOR" in
    "test"|"jetaccess")
        ;;
    *)
        echo -e "${RED}Error: Unsupported operator '$OPERATOR'${NC}"
        echo "Supported operators: test, jetaccess"
        exit 1
        ;;
esac

# Set operator-specific environment variables
case "$OPERATOR" in
    "test")
        export ENVIRONMENT="test"
        ;;
    "jetaccess")
        export ENVIRONMENT="jetaccess"
        ;;
esac

echo -e "${BLUE}Running Avianis ETL for operator: $OPERATOR${NC}"

# Check if virtual environment exists and activate it
if [ -d "venv" ]; then
    echo -e "${BLUE}Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${YELLOW}Warning: Virtual environment not found. Make sure dependencies are installed.${NC}"
fi

# Function to display usage
usage() {
    echo -e "${BLUE}Avianis ETL Pipeline Runner${NC}"
    echo
    echo "Usage: $0 [OPERATOR] [COMMAND] [OPTIONS]"
    echo
    echo "Operators (defaults to 'test' if not specified):"
    echo "  test                    Use test environment"
    echo "  jetaccess               Use jetaccess environment"
    echo
    echo "Commands:"
    echo "  full                    Run complete ETL pipeline (default)"
    echo "  setup                   Initial data setup: Load aircraft types, categories, aircraft, and crew (run once for new operators)"
    echo "  aircraft                Load aircraft data only"
    echo "  crew                    Load crew and personnel data only"  
    echo "  flight-data             Load flight legs and crew assignments only"
    echo "  personnel-events        Load personnel events only"
    echo
    echo "Examples:"
    echo "  $0 test full                         # Run complete ETL for test operator"
    echo "  $0 thrive setup                      # Load aircraft and crew data for thrive"
    echo "  $0 test aircraft                     # Load aircraft data only for test"
    echo "  $0 thrive crew                       # Load crew data only for thrive"
    echo
}

# Function to log and execute commands with status tracking
run_command() {
    local cmd="$1"
    local desc="$2"
    local log_file="$3"
    
    echo "[$desc] Running: $cmd" | tee -a "$log_file"
    
    if eval "$cmd" 2>&1 | tee -a "$log_file"; then
        echo "[$desc] SUCCESS" | tee -a "$log_file"
        return 0
    else
        echo "[$desc] FAILED" | tee -a "$log_file"
        return 1
    fi
}

# Function to run ETL with comprehensive logging
run_etl() {
    local cmd="$1"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_dir="logs/${OPERATOR}"
    local log_file="${log_dir}/etl_run_${timestamp}.log"
    
    # Create operator-specific log directory if it doesn't exist
    mkdir -p "$log_dir"
    
    # Initialize log with header
    echo "================================================" | tee -a "$log_file"
    echo "Avianis ETL Run Started: $(date)" | tee -a "$log_file"
    echo "Operator: $OPERATOR" | tee -a "$log_file"
    echo "Command: $cmd" | tee -a "$log_file"
    echo "================================================" | tee -a "$log_file"
    
    echo -e "${BLUE}Starting ETL operation for $OPERATOR${NC}"
    echo -e "${BLUE}Log file: $log_file${NC}"
    
    # Run the main ETL pipeline with timing
    echo "" | tee -a "$log_file"
    echo "Running ETL Pipeline..." | tee -a "$log_file"
    
    local start_time=$(date +%s)
    local etl_cmd="python3 etl_pipeline.py --operator $OPERATOR $cmd"
    
    if run_command "$etl_cmd" "ETL PIPELINE" "$log_file"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        echo "" | tee -a "$log_file"
        echo "================================================" | tee -a "$log_file"
        echo "ETL COMPLETED SUCCESSFULLY" | tee -a "$log_file"
        echo "Duration: ${duration} seconds" | tee -a "$log_file"
        echo "Completed: $(date)" | tee -a "$log_file"
        echo "================================================" | tee -a "$log_file"
        
        echo -e "${GREEN}ETL operation completed successfully for $OPERATOR (${duration}s)${NC}"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        echo "" | tee -a "$log_file"
        echo "================================================" | tee -a "$log_file"
        echo "ETL FAILED" | tee -a "$log_file"
        echo "Duration: ${duration} seconds" | tee -a "$log_file"
        echo "Failed: $(date)" | tee -a "$log_file"
        echo "================================================" | tee -a "$log_file"
        
        echo -e "${RED}ETL operation failed for $OPERATOR after ${duration}s. Check $log_file for details${NC}"
        return 1
    fi
}

# Main script logic
case "${1:-full}" in
    "help"|"-h"|"--help")
        usage
        exit 0
        ;;
    "setup")
        run_etl "--setup"
        ;;
    "full")
        run_etl ""
        ;;
    "aircraft")
        run_etl "--aircraft-only"
        ;;
    "crew")
        run_etl "--crew-only"
        ;;
    "flight-data")
        run_etl "--flight-data-only"
        ;;
    "crew-events")
        run_etl "--crew-events-only"
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$1'${NC}"
        echo
        usage
        exit 1
        ;;
esac