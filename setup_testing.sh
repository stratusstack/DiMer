#!/bin/bash

# DiMer Snowflake Connector Setup Script
# This script installs all dependencies needed for testing

set -e  # Exit on any error

echo "🚀 Setting up DiMer Snowflake Connector for testing..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if Python 3.9+ is available
echo "Checking Python version..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
    echo "Python version: $PYTHON_VERSION"
    
    # Check if version is 3.9 or higher
    if python3 -c "import sys; exit(0 if sys.version_info >= (3,9) else 1)"; then
        print_success "Python 3.9+ detected"
    else
        print_error "Python 3.9+ required, found $PYTHON_VERSION"
        exit 1
    fi
else
    print_error "Python 3 not found. Please install Python 3.9+"
    exit 1
fi

# Install core package with Snowflake and dev dependencies
echo -e "\n📦 Installing DiMer with dependencies..."

# Install Snowflake connector first (avoid dependency conflicts)
print_info "Installing Snowflake connector..."
pip install 'snowflake-connector-python[pandas,secure-local-storage]>=3.0.0'

if [ $? -eq 0 ]; then
    print_success "Snowflake connector installed"
else
    print_error "Failed to install Snowflake connector"
    exit 1
fi

# Install the package in development mode with dev dependencies
print_info "Installing DiMer in development mode..."
pip install -e ".[dev]"

if [ $? -eq 0 ]; then
    print_success "DiMer installed in development mode"
else
    print_error "Failed to install DiMer"
    exit 1
fi

# Check if .env file exists
echo -e "\n🔧 Checking environment configuration..."
if [ -f ".env" ]; then
    print_success ".env file found"
else
    print_warning ".env file not found"
    
    if [ -f ".env.example" ]; then
        print_info "Creating .env from .env.example..."
        cp .env.example .env
        print_warning "Please edit .env file with your Snowflake credentials"
        print_info "Required variables:"
        echo "  - SNOWFLAKE_ACCOUNT"
        echo "  - SNOWFLAKE_USER" 
        echo "  - SNOWFLAKE_PASSWORD"
        echo "  - SNOWFLAKE_DATABASE"
        echo "  - SNOWFLAKE_SCHEMA"
        echo "  - SNOWFLAKE_WAREHOUSE"
        echo "  - SNOWFLAKE_ROLE"
    else
        print_error ".env.example file not found"
        print_info "Please create a .env file with your Snowflake credentials"
    fi
fi

# Make the test script executable
print_info "Making test script executable..."
chmod +x test_snowflake.py

print_success "Setup completed successfully!"

echo -e "\n📋 Next steps:"
echo "1. Edit .env file with your Snowflake credentials (if not done already)"
echo "2. Run the test script:"
echo "   python test_snowflake.py                    # Full test suite"
echo "   python test_snowflake.py --unit-only        # Unit tests only" 
echo "   python test_snowflake.py --integration-only # Integration tests only"
echo "   python test_snowflake.py --benchmark        # Include performance benchmark"
echo ""
echo "3. For quick manual testing:"
echo "   python -c \"from dimer.connectors.snowflake.connector import SnowflakeConnector; print('Import successful!')\""

print_success "🎉 Setup complete! Ready for testing."