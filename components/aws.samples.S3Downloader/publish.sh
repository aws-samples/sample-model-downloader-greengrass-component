#!/bin/bash

set +e

# Function to detect OS type
get_os_type() {
    case "$(uname -s)" in
        Darwin*)    echo "macos";;
        Linux*)     echo "linux";;
        *)         echo "unknown";;
    esac
}

# Function to get the system's hostname
get_hostname() {
    hostname=$(hostname)
    if [ -z "$hostname" ] && [ -f /etc/hostname ]; then
        hostname=$(cat /etc/hostname)
    fi
    echo "$hostname"
}

# Function for OS-specific sed command
do_sed_replace() {
    local file=$1
    local search=$2
    local replace=$3
    local os_type=$(get_os_type)

    if [ "$os_type" = "macos" ]; then
        sed -i '' "s|$search|$replace|g" "$file"
    elif [ "$os_type" = "linux" ]; then
        sed -i "s|$search|$replace|g" "$file"
    else
        echo "Unsupported operating system"
        exit 1
    fi
}

# Print OS type for debugging
OS_TYPE=$(get_os_type)
echo "Detected OS: $OS_TYPE"

UNIQUE_NAME_REPLACEMENT=".u-$(get_hostname)"

if [ -f "./gdk-config.json" ]; then
    echo "Processing gdk-config.json"
    do_sed_replace "gdk-config.json" "{UNIQUE_NAME}" "$UNIQUE_NAME_REPLACEMENT"
else
    echo "gdk-config.json not found in the current directory"
fi

echo "Publishing component..."
gdk component publish
GDK_EXIT_STATUS=$?
echo "GDK_EXIT_STATUS: $GDK_EXIT_STATUS"

if [ -f "./gdk-config.json" ]; then
    echo "Processing gdk-config.json"
    do_sed_replace "gdk-config.json" "$UNIQUE_NAME_REPLACEMENT" "{UNIQUE_NAME}"
else
    echo "gdk-config.json not found in the current directory"
fi

set -e
exit "${GDK_EXIT_STATUS}"