#!/bin/bash
set -e

# Function to print messages with timestamp
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check and install s5cmd if needed
install_s5cmd() {
    local required_version=$1
    
    if ! command -v s5cmd &> /dev/null; then
        log_message "s5cmd not found, will install"
        INSTALL_S5CMD=true
    else
        # Extract version from s5cmd version output (e.g., "v2.3.0-991c9fb")
        CURRENT_VERSION=$(s5cmd version | cut -d'-' -f1 | tr -d 'v')
        if [ "$CURRENT_VERSION" != "$required_version" ]; then
            log_message "s5cmd version mismatch. Current: $CURRENT_VERSION, Required: $required_version"
            INSTALL_S5CMD=true
        else
            log_message "s5cmd version $required_version already installed"
            return 0
        fi
    fi

    if [ "$INSTALL_S5CMD" = true ]; then
        log_message "Installing s5cmd version $required_version"
        
        # Determine architecture
        ARCH=$(uname -m)
        if [ "$ARCH" = "x86_64" ]; then
            ARCH="64bit"
        elif [ "$ARCH" = "aarch64" ]; then
            ARCH="arm64"
        else
            log_message "Unsupported architecture: $ARCH"
            return 1
        fi
        
        # Download and install s5cmd
        S5CMD_URL="https://github.com/peak/s5cmd/releases/download/v${required_version}/s5cmd_${required_version}_Linux-${ARCH}.tar.gz"
        log_message "Downloading s5cmd from: $S5CMD_URL"
        
        # Clean up any existing downloaded files
        rm -f s5cmd_*.tar.gz s5cmd
        
        wget "$S5CMD_URL"
        tar -xzf "s5cmd_${required_version}_Linux-${ARCH}.tar.gz"
        chmod +x s5cmd
        sudo mv s5cmd /usr/local/bin/
        rm -f "s5cmd_${required_version}_Linux-${ARCH}.tar.gz"
        
        log_message "s5cmd installation completed"
    fi
}

# Main execution
if [ "$#" -ne 1 ]; then
    log_message "Error: Required s5cmd version not provided"
    echo "Usage: $0 <s5cmd_version>"
    exit 1
fi

install_s5cmd "$1"
