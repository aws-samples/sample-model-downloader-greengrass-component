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

files=$(find . -maxdepth 1 -type f -not -name "*.sh")

# Set the locale to UTF-8
export LC_ALL=C
# First pass: Replace placeholders
echo "Replacing placeholders..."
for file in $files; do
    echo "$file"
    filename=$(sed 's/^\.\///' <<< $file)

    # Replace ECR repo placeholder
    do_sed_replace "$filename" "{ECR_REPO}" "$ECR_REPO"
    
    # Replace unique name placeholder in specific files
    if [ "$filename" == "gdk-config.json" ] || [ "$filename" == "recipe.yaml" ]; then
        do_sed_replace "$filename" "{UNIQUE_NAME}" "$UNIQUE_NAME_REPLACEMENT"
    fi
done

echo "Building component..."
gdk component build

# Second pass: Restore placeholders
echo "Restoring placeholders..."
files=$(find . -maxdepth 1 -type f -not -name "*.sh")

# Loop through each file
for file in $files; do
    filename=$(sed 's/^\.\///' <<< $file)

    # Restore ECR repo placeholder
    do_sed_replace "$filename" "$ECR_REPO" "{ECR_REPO}"
    
    # Restore unique name placeholder in specific files
    if [ "$filename" == "gdk-config.json" ] || [ "$filename" == "recipe.yaml" ]; then
        do_sed_replace "$filename" "$UNIQUE_NAME_REPLACEMENT" "{UNIQUE_NAME}"
    fi
done

if [ $? -eq 0 ]; then
    echo "Build completed successfully"
else
    echo "Build failed"
    exit 1
fi

set -e
