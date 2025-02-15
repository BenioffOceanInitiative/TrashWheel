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

BUCKET_NAME = os.environ.get("BUCKET_NAME") 
TRASH_WHEELS_STR = os.environ.get('TRASH_WHEELS', '["1", "2", "3", "4", "5"]')
TRASH_WHEELS = json.loads(TRASH_WHEELS_STR)
INSTANCE_TEMPLATE_NAME = os.environ.get("INSTANCE_TEMPLATE_NAME")

CVAT_PASSWORD = os.environ.get("CVAT_PASSWORD")
CVAT_USERNAME = os.environ.get("CVAT_USERNAME")

def start_inference_vm(valid_folders_for_inference, date):
    """Provisions a VM from an instance template identified in name by the date, passing in Metadata key "folders" to run inference on

    Args:
        valid_folders_for_inference (List[str]): List of folder paths from base bucket for images. i.e. ["1/2025-2-1/", ...]
        date (str): Yesterday's date in YYYY-M-D format. i.e. "2025-2-1"
    Returns:
        A VM creation operation that is still waiting to finish. Base code from https://cloud.google.com/compute/docs/instances/create-vm-from-instance-template in the "With overrides" section.
    """
    project_id = "cleancurrentscoalition"
    zone = "us-central1-a"
    instance_name = f"baltimore-auto-annotation-{date}"

    instance_client = compute_v1.InstancesClient()
    instance_template_client = compute_v1.InstanceTemplatesClient()

    # Retrieve an instance template by name.
    instance_template = instance_template_client.get(
        project=project_id, 
        instance_template=INSTANCE_TEMPLATE_NAME
    )

    # Build a new Instance object. We copy properties from the template
    # but override *only* metadata.
    instance = compute_v1.Instance()
    instance.name = instance_name
    # Use the machineType from the template
    machine_type_in_template = instance_template.properties.machine_type

    # Transform machine type to fully qualified URL
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

    # Merge metadata: Preserve what's in the template, adding our 'folders' and cvat environment variable keys to it.
    merged_items = []
    existing_metadata = instance_template.properties.metadata

    if existing_metadata and existing_metadata.items:
        merged_items = list(existing_metadata.items)

    merged_items = [item for item in merged_items if item.key != "folders"]

    merged_items.append(
        compute_v1.Items(
            key="folders",
            value=json.dumps(valid_folders_for_inference)  
        )
    )

    merged_items.append(
        compute_v1.Items(
            key="cvat_username",
            value=CVAT_USERNAME
        )
    )
    merged_items.append(
        compute_v1.Items(
            key="cvat_password",
            value=CVAT_PASSWORD
        )
    )

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
    """HTTP Cloud Function. Checks if there are new images in Cloud Storage to be annotate from yesterday's date in any of the trash_wheels.

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

    bucket = storage_client.bucket(BUCKET_NAME)

    # Calculate yesterday's date in YYYY-m-d format
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%-m-%-d")

    valid_folders_for_inference = []

    # Iterate over each folder
    for wheel in TRASH_WHEELS:
        # Construct the expected path for yesterday's date
        date_wheel_path = f"{wheel}/{yesterday}/"

        print("Now processing: ", date_wheel_path)

        images_folder_path = f"{date_wheel_path}images/"
        images_blobs = storage_client.list_blobs(BUCKET_NAME, prefix=images_folder_path)

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
        auto_annotations_blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=auto_annotations_folder_path, max_results=1))

        if auto_annotations_blobs: # This insures that there's at least one file in there. However, check manifest.json TODOs for this as this is not the best solution.
            print(f"'auto-annotations' folder already contains files in '{date_wheel_path}'.")
            continue

        valid_folders_for_inference.append(date_wheel_path)

    # If there are new images, spin up VM and run inference to get automatic annotations on them.
    if len(valid_folders_for_inference) > 0:
        print(f"Now processing: {valid_folders_for_inference}", yesterday)
        start_inference_vm(valid_folders_for_inference, yesterday)

    else:
        print("Did not find any valid data to run inference on or the dates already have a folder named auto-annotations")

    return "Processing complete", 200

