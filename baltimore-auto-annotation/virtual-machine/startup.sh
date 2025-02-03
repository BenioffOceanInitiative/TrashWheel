#! /bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# ================================
# Function to Delete the Instance
# ================================
delete_instance() {
    echo "Initiating instance deletion..."

    # Retrieve project ID, zone, and instance name from metadata
    PROJECT=$(curl -s -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/project/project-id)
    ZONE=$(curl -s -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
    INSTANCE=$(curl -s -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/name)
    
    # Obtain an access token using the VM's service account
    TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
        http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token | \
        python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")
    
    # Send a DELETE request to the Compute Engine API to delete the instance
    curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
        "https://compute.googleapis.com/compute/v1/projects/$PROJECT/zones/$ZONE/instances/$INSTANCE"
}

# ==================================
# Trap to Ensure Instance Deletion
# ==================================
trap delete_instance EXIT


# ==================================
# Wait for nvidia-smi to be available to avoid race conditions with apt
# ==================================
MAX_WAIT_TIME=600
INTERVAL=10
elapsed_time=0

while ! command -v nvidia-smi &> /dev/null; do
    if [ $elapsed_time -ge $MAX_WAIT_TIME ]; then
        echo "Error: Timeout reached. NVIDIA driver installation did not complete within 10 minutes."
        exit 1
    fi

    echo "Waiting for NVIDIA driver to be available..."
    sleep $INTERVAL
    ((elapsed_time+=INTERVAL))
done

# =============================
# Step 1: Retrieve 'folders' Metadata
# =============================
echo "Fetching 'folders' metadata..."
FOLDERS=$(curl -s -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/attributes/folders)

echo "Folders metadata: $FOLDERS"

# Export the FOLDERS variable for use in scripts
export FOLDERS

# =============================
# Step 2: Install Required Packages
# =============================
echo "Updating package lists..."
apt-get update

echo "Installing required packages: python3, python3-pip, apache2, gnupg..."
apt-get install -y python3 python3-pip apache2 gnupg

# =============================
# Step 3: Install Google Cloud SDK
# =============================
echo "Adding Google Cloud SDK repository..."
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
    tee /etc/apt/sources.list.d/google-cloud-sdk.list

echo "Importing Google Cloud public key..."
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor | \
    tee /usr/share/keyrings/cloud.google.gpg > /dev/null

echo "Updating package lists after adding Cloud SDK repository..."
apt-get update

echo "Installing Google Cloud SDK..."
apt-get install -y google-cloud-sdk

# =============================
# Step 4: Download Python Scripts from GCS
# =============================
echo "Downloading Python scripts from GCS..."

# Define destination directory
SCRIPT_DIR="/usr/local/bin"
mkdir -p $SCRIPT_DIR


# Download scripts using gsutil
gsutil cp gs://trashwheel_baltimore_auto_annotation_test/scripts/inference.py $SCRIPT_DIR/inference.py
gsutil cp gs://trashwheel_baltimore_auto_annotation_test/scripts/requirements.txt $SCRIPT_DIR/requirements.txt
gsutil cp gs://trashwheel_baltimore_auto_annotation_test/scripts/upload_to_cvat.py $SCRIPT_DIR/upload_to_cvat.py

# Install dependencies from requirements.txt
pip install --no-cache-dir -r $SCRIPT_DIR/requirements.txt

# Make the scripts executable
chmod +x $SCRIPT_DIR/inference.py
chmod +x $SCRIPT_DIR/upload_to_cvat.py

# =============================
# Step 5: Execute the Python Scripts
# =============================
echo "Executing inference.py..."
python3 $SCRIPT_DIR/inference.py "$FOLDERS"

# echo "Executing upload_to_cvat.py..."
# python3 $SCRIPT_DIR/upload_to_cvat.py --folders=$FOLDERS

# =============================
# Completion Message
# =============================
echo "All scripts executed successfully. The instance will now delete itself."

# The trap set earlier will handle the deletion of the instance.