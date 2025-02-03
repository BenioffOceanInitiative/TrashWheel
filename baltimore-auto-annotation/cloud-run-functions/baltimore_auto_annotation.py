import functions_framework
from google.cloud import storage
from datetime import datetime, timedelta
import os
from google.cloud import compute_v1
import googleapiclient.discovery
import json
from googleapiclient.discovery import build


# Valid ultralytics/yolov11 image formats
IMG_FORMATS = {"bmp", "dng", "jpeg", "jpg", "mpo", "png", "tif", "tiff", "webp", "pfm", "heic"}

bucket_name = os.environ.get("BUCKET_NAME") 
trash_wheels_str = os.environ.get("TRASH_WHEELS", "[1, 2, 3]")
trash_wheels = json.loads(trash_wheels_str)
instance_template_name = os.environ.get("INSTANCE_TEMPLATE_NAME")

def start_inference_vm(valid_folders_for_inference, date):
    """Spins up a VM from an instance template identified in name by the date, passing in Metadata for FOLDERS to run inference on

    Args:
        valid_folders_for_inference (List[str]): List of folder paths from base bucket for images. i.e. ["1/2025-2-1/", ...]
        date (str): Yesterday's date in YYYY-M-D format. i.e. "2025-2-1"
    Returns:
        An VM creation operation that is still waiting to finish. Base code from https://cloud.google.com/compute/docs/instances/create-vm-from-instance-template in the "With overrides" section.
    """
    project_id = "cleancurrentscoalition"
    zone = "us-central1-a"
    instance_name = f"baltimore-auto-annotation-{date}"

    instance_client = compute_v1.InstancesClient()
    instance_template_client = compute_v1.InstanceTemplatesClient()

    # Retrieve an instance template by name.
    instance_template = instance_template_client.get(
        project=project_id, 
        instance_template=instance_template_name
    )

    # Build a new Instance object. We copy properties from the template
    # but override *only* metadata.
    instance = compute_v1.Instance()
    instance.name = instance_name
    # Use the machineType from the template
    machine_type_in_template = instance_template.properties.machine_type
    if not instance_template.properties.machine_type.startswith("projects/"):
        machine_type_in_template = f"projects/{project_id}/zones/{zone}/machineTypes/{machine_type_in_template}"
    instance.machine_type = machine_type_in_template
    
    # Still need to transform names of disk types into their fully qualified URLs
    for disk in instance_template.properties.disks:
        if disk.initialize_params.disk_type:
            disk.initialize_params.disk_type = (
                f"zones/{zone}/diskTypes/{disk.initialize_params.disk_type}"
            )

    # Use the disks from the template
    instance.disks = list(instance_template.properties.disks)

    # Merge metadata: Preserve what's in the template and add our 'folders' key.
    merged_items = []
    existing_metadata = instance_template.properties.metadata

    # Copy existing items if they exist.
    if existing_metadata and existing_metadata.items:
        # Convert to a list so we can manipulate easily
        merged_items = list(existing_metadata.items)

    # If you want to override the "folders" key if it exists, remove any existing "folders" item:
    merged_items = [item for item in merged_items if item.key != "folders"]

    # Now append our "folders" item
    merged_items.append(
        compute_v1.Items(
            key="folders",
            value=json.dumps(valid_folders_for_inference)  
        )
    )

    # Assign the merged items back to the instance metadata
    instance.metadata = compute_v1.Metadata(items=merged_items)

   
    # Prepare the insert request
    insert_request = compute_v1.InsertInstanceRequest()
    insert_request.project = project_id
    insert_request.zone = zone
    insert_request.instance_resource = instance
    # Pass the original template as the source
    insert_request.source_instance_template = instance_template.self_link

    # Create the instance
    operation = instance_client.insert(insert_request)

    print(f"Created VM instance '{instance_name}' with metadata.")
    return operation


@functions_framework.http
def main(request):
    """HTTP Cloud Function. Checks if there are new images in Cloud Storage to be annotate from yesterday's date in any of the trash_wheels..

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    # Initialize the Cloud Storage client
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Calculate yesterday's date in YYYY-m-d format
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%-m-%-d")

    valid_folders_for_inference = []

    # Iterate over each folder
    for wheel in trash_wheels:
        # Construct the expected path for yesterday's date
        date_wheel_path = f"{wheel}/{yesterday}/"

        print("Now processing: ", date_wheel_path)

        images_folder_path = f"{date_wheel_path}images/"
        images_blobs = storage_client.list_blobs(bucket_name, prefix=images_folder_path)

        # Check if all files are supported image types through basic extension checking
        valid_images = True
        at_least_one_file = False
        for blob in images_blobs:
            at_least_one_file = True
            extension = blob.name.lower().split('.')[-1]
            if extension not in IMG_FORMATS:
                valid_images = False
                break

        if not at_least_one_file:
            print(f"No files found in {date_wheel_path}")
            continue

        if not valid_images:
            print(f"An invalid file format was found in {date_wheel_path}")
            continue

        # Check if there are any images under the 'auto-annotations' folder
        auto_annotations_folder_path = f"{date_wheel_path}auto-annotations/"
        auto_annotations_blobs = list(storage_client.list_blobs(bucket_name, prefix=auto_annotations_folder_path, max_results=1))

        if auto_annotations_blobs:  # This ensures there's at least one object in the folder
            print(f"'auto-annotations' folder already contains files in '{date_wheel_path}'.")
            continue
            
        # Check if there are any images under the 'auto-annotations' folder
        auto_annotations_folder_path = f"{date_wheel_path}auto-annotations/"
        auto_annotations_blobs = list(storage_client.list_blobs(bucket_name, prefix=auto_annotations_folder_path, max_results=2))

        if any(blob.name != auto_annotations_folder_path for blob in auto_annotations_blobs):  
            # Ensures at least one object exists in the folder that is not just the folder itself
            print(f"'auto-annotations' folder already contains files in '{date_wheel_path}'.")
            continue

        valid_folders_for_inference.append(date_wheel_path)

    # If there are new images, spin up VM and run inference to get automatic annotations on them.
    if len(valid_folders_for_inference) > 0:
        print(valid_folders_for_inference, yesterday)
        start_inference_vm(valid_folders_for_inference, yesterday)

    else:
        print("Did not find any valid data to run inference on or the date already has a folder named auto-annotations")

    return "Processing complete", 200

