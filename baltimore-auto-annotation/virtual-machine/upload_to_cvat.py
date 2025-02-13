import argparse
import json
from CVAT.cvat_client import CVATClient
from google.cloud import storage
import google.cloud.logging
# Initialize Cloud Logging client
client = google.cloud.logging.Client()
client.setup_logging()
import logging

BUCKET_NAME = "/trashwheel"

if __name__ == "__main__":

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        description="Upload images and annotations from GCS to CVAT"
    )
    parser.add_argument(
        "folders",
        type=str,
        help=("JSON string of folder paths in gs://BUCKET_NAME/ containing the images. "
              "Example: '[\"1/2023-1-1/\"]'")
    )
    args = parser.parse_args()

    client = CVATClient(bucket_mount_path=BUCKET_NAME)

    # The script expects a JSON string representing a list of folder paths.
    folder_paths = json.loads(args.folders)

    try: 
        for folder_path in folder_paths:
            device_id, date, discard = folder_path.split("/")
            if client.upload_to_cvat(device_id=device_id, date=date):
                logging.info(f"baltimore-auto-annotation: Upload to CVAT succeeded for {device_id} {date}")
            else: 
                logging.exception(f"baltimore-auto-annotation: Upload to CVAT script failed unexpectedly for {device_id} {date}")
    except Exception as e:
        logging.exception(f"baltimore-auto-annotation {folder_paths}: Upload to CVAT script failed unexpectedly due to {e}")
        raise




