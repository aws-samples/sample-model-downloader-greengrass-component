#!/bin/bash
###############################################################################
# Description: Compare hash checksum of the files in component to detect changes
#              It also check any docker image changes used in the URI secion of
#              the recipe.yaml file in each component root.
# Args: none
###############################################################################

# set +e

source .env

extract_image_names() {
    local file_path="$1"
    local image_names=()
    while IFS= read -r line; do
        if expr "'$line'" : '.*- URI:.*docker:\(.*\).' >/dev/null; then
            image_name=$(echo "$line" | awk -F'[/:]' '{print $(NF-1)}')
            image_names+=("$image_name")
        fi
    done < "$file_path"

    echo "${image_names[@]}"
}

# create checksums directory if not exists
folder_name="checksums"

if [ ! -d "./components/$folder_name" ]; then
    mkdir "./components/$folder_name"
    echo "Created directory: ./components/$folder_name"
fi

if [ -d "./docker" ]; then
    # Only create checksums subdirectory if docker directory exists
    if [ ! -d "./docker/$folder_name" ]; then
        mkdir "./docker/$folder_name"
        echo "Created directory: ./docker/$folder_name"
    fi
else
    echo "Docker directory not found. Skipping Docker checksum directory creation."
fi

# Process each component directory only when there was a change
# Exclude directorychecksum folder from processing
for DIR in $(ls -d components/*/ | grep -v 'components/checksums' | sed 's/components\///' | sed 's/\///')
do
    # Store the previous hashes in a file
    echo "DIR: $DIR"
    IS_DOCKER_HASH_CHANGED=false
    COMPONENT_PATH="components/$DIR"
    HASH_FILE="components/checksums/$DIR.txt"

    # Load the previous hash from the file
    if [ -f "$HASH_FILE" ]; then
        read -r PREV_HASH < "$HASH_FILE"
    else
        PREV_HASH=""
    fi
    echo "PREV_HASH: $PREV_HASH"

    # Exclude folders with -build in the name. Those are GDK build folders.
    CURR_HASH=$(find "$COMPONENT_PATH" -not -path '*-build/*' -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
    echo "CURR_HASH: $CURR_HASH"

    ############################################## docker
    # Get docker image list used in the recipe.yaml file
    RECIPE_FILE="$COMPONENT_PATH/greengrass-build/recipes/recipe.yaml"
    if [ -f "$RECIPE_FILE" ]; then
        IMAGE_NAMES=($(extract_image_names "$RECIPE_FILE"))
        echo "Image names: ${IMAGE_NAMES[@]}"
    fi

    # Check if the array is not empty
    if [ ${#IMAGE_NAMES[@]} -ne 0 ]; then
        echo "Processing docker hash"

        # Check if docker directory exists
        if [ ! -d "./docker" ]; then
            echo "ERROR: Docker directory doesn't exist. Cannot continue with Docker-dependent component."
            exit 1
        fi

        for IMAGE_NAME in "${IMAGE_NAMES[@]}"; do
            # Check if the specific docker image directory exists
            if [ ! -d "./docker/$IMAGE_NAME" ]; then
                echo "WARNING: Docker directory for $IMAGE_NAME doesn't exist. Skipping Docker build."
                continue  # Skip to the next Docker image
            fi

            # Load the docker previous hash from the file
            DOCKER_HASH_FILE="docker/checksums/$IMAGE_NAME.txt"
            if [ -f "$DOCKER_HASH_FILE" ]; then
                read -r DOCKER_PREV_HASH < "$DOCKER_HASH_FILE"
            else
                DOCKER_PREV_HASH=""
            fi
            echo "DOCKER_PREV_HASH: $DOCKER_PREV_HASH"

            DOCKER_PATH="docker/$IMAGE_NAME"
            if [ -d "$DOCKER_PATH" ]; then
                DOCKER_CURR_HASH=$(find "$DOCKER_PATH" -not -path '.DS_Store' -not -name 'build.sh' -not -name 'publish.sh' -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
                echo "DOCKER_CURR_HASH: $DOCKER_CURR_HASH"
            else
                DOCKER_CURR_HASH=""
            fi

            # Check if the current hash matches the previous hash
            if [ "$DOCKER_CURR_HASH" != "$DOCKER_PREV_HASH" ]; then
                IS_DOCKER_HASH_CHANGED=true

                echo "Publishing $IMAGE_NAME"
                pushd "docker/$IMAGE_NAME"
                ########## Publish docker ##########
                (
                    source ./publish.sh
                )
                PUBLISH_EXIT_STATUS=$?
                echo "PUBLISH_EXIT_STATUS: $PUBLISH_EXIT_STATUS"
                popd
                if [ "$PUBLISH_EXIT_STATUS" -eq 0 ]; then
                    IS_DOCKER_HASH_CHANGED=true
                    echo "$DOCKER_CURR_HASH" > "$DOCKER_HASH_FILE"
                    echo "Docker checksum saved: $DOCKER_HASH_FILE"
                else
                    echo "Failed to publish Docker image: $IMAGE_NAME"
                    exit 1
                fi
                ########## Publish docker ##########
                echo "Docker image: $IMAGE_NAME publish complete"
            fi
        done
    fi
    ############################################## Docker

    # Check if the current hash matches the previous hash
    if [ "$CURR_HASH" != "$PREV_HASH" ] || [ "$IS_DOCKER_HASH_CHANGED" = true ]; then
        echo "Publishing $DIR"
        pushd "$COMPONENT_PATH"
        ########## Publish component ##########
        (
            source ./publish.sh
        )
        PUBLISH_EXIT_STATUS=$?
        echo "PUBLISH_EXIT_STATUS: $PUBLISH_EXIT_STATUS"
        popd
        if [ $PUBLISH_EXIT_STATUS -eq 0 ]; then
            echo "$CURR_HASH" > "$HASH_FILE"
            echo "Component checksum saved: $HASH_FILE"
        else
            echo "Failed to publish component: $DIR"
            exit 1
        fi
        ########## Publish component ##########
        echo "Component: $DIR publish complete"
    else
        echo "No changes to publish for $DIR"
    fi
done

echo "########## FINISHED GREENGRASS PUBLISH ##########"

echo "All publishing complete!"
