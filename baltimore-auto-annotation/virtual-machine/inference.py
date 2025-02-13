import os
import argparse
import tempfile
import shutil
import json
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from ultralytics import YOLO
from pathlib import Path
import re
import google.cloud.logging

# Initialize Cloud Logging client
client = google.cloud.logging.Client()
client.setup_logging()

import logging

# Allowed image extensions for ultralytic's YOLOv11 (on their Github)
IMG_FORMATS = (".bmp", ".dng", ".jpeg", ".jpg", ".mpo", ".png", ".tif", ".tiff", ".webp", ".pfm", ".heic")


# Default YOLOv11 model confidence is 0.25
CONFIDENCE = 0.25

BUCKET_NAME = "trashwheel"

def download_gcs_folder(bucket_name, source_folder, destination_folder, storage_client):
    """
    Download all files from a GCS folder to a local directory, preserving the directory structure.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_folder)
    for blob in blobs:
        # Skip if the blob represents a directory
        if blob.name.endswith('/'):
            continue
        # Compute the relative path and create any needed local directories
        relative_path = os.path.relpath(blob.name, source_folder)
        destination_path = os.path.join(destination_folder, relative_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        blob.download_to_filename(destination_path)
    print(f"Downloaded folder '{source_folder}' from bucket '{bucket_name}' to '{destination_folder}'.")


def list_gcs_images(bucket_name, images_folder, storage_client):
    """
    List all image file paths in the specified GCS images folder.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=images_folder)
    image_paths = [blob.name for blob in blobs if blob.name.lower().endswith(IMG_FORMATS)]
    print(f"Found {len(image_paths)} images in gs://{bucket_name}/{images_folder}")
    return image_paths


def process_batch(model, input_dir, output_dir):
    """
    Run inference on all images in the given input_dir and move the resulting
    annotated text files to output_dir.
    """
    # Run inference on all images in input_dir.
    model.predict(
        source=str(input_dir),
        save=True,
        save_txt=True, # This will output text file annotations for any image that has >= 1 detected class
        imgsz=640,
        device=0,
        batch=16,
        conf=CONFIDENCE
    )

    # The annotated text files are saved in 'runs/detect/predict/labels'
    predicted_dir = Path("runs/detect/predict")
    predicted_txt_dir = predicted_dir / "labels"
    if predicted_txt_dir.exists():
        for file in predicted_txt_dir.iterdir():
            if file.is_file() and file.suffix == ".txt":
                shutil.move(str(file), os.path.join(output_dir, file.name))
        shutil.rmtree(predicted_dir)

    # Create an empty .txt file for every image that didn't get a label file
    for image_file in Path(input_dir).iterdir():
        if image_file.suffix.lower() in IMG_FORMATS:
            label_file_name = image_file.stem + ".txt"
            label_file_path = Path(output_dir) / Path(label_file_name)

            if not label_file_path.exists():
                label_file_path.touch()

def get_latest_model_version(bucket_name, production_folder, storage_client):
    """
    Returns the latest model version directory under the production_folder in GCS.
    Expects directories in production_folder to be named in the format 'model_v{number}/'.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=production_folder)  # No delimiter to list everything

    model_versions = []
    for blob in blobs:
        # Extract directory name from blob's full path
        match = re.search(r'model_v(\d+)/', blob.name)
        if match:
            version = int(match.group(1))
            model_versions.append((version, f"{production_folder}model_v{version}/"))

    if not model_versions:
        raise ValueError("No model versions found in the production folder.")

    latest_version, latest_model_prefix = max(model_versions, key=lambda x: x[0])
    print(f"Found latest model version: {latest_version} in directory '{latest_model_prefix}'")
    return latest_model_prefix

def main(folder_path):
    """
    Main workflow:
      - Download the model files from GCS.
      - List the images in the provided folder.
      - Process the images in batches using YOLO for inference.
      - Upload each batch of annotated text files back to GCS.
      - Repeat.
    """
    # Initialize the Google Cloud Storage client.
    storage_client = storage.Client()

    bucket_name = BUCKET_NAME
    model_gcs_path = get_latest_model_version(bucket_name, "models/production/", storage_client)
    
    images_gcs_path = os.path.join(folder_path, "images/")
    annotated_gcs_path = os.path.join(folder_path, "auto-annotations/")

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Download the model files from GCS.
        download_gcs_folder(bucket_name, model_gcs_path, tmp_dir, storage_client)
        
        # Find the model path.
        model_path = os.path.join(tmp_dir, "weights", "best.pt")

        # Load the YOLO model.
        print("Loading YOLO model...")
        model = YOLO(model_path)
        print("Model loaded successfully.")

        # List all images in the specified GCS folder.
        image_paths = list_gcs_images(bucket_name, images_gcs_path, storage_client)
        if not image_paths:
            print("No images found to process.")
            return

        # Prepare local directories for input images and annotated outputs.
        input_dir = os.path.join(tmp_dir, "input_images")
        output_dir = os.path.join(tmp_dir, "annotated_images")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        BATCH_SIZE = 16  # Adjust the batch size based on GPU memory.
        for batch_index in range(0, len(image_paths), BATCH_SIZE):
            batch = image_paths[batch_index: batch_index + BATCH_SIZE]
            print(f"Processing batch {batch_index // BATCH_SIZE + 1} with {len(batch)} images.")

            # Download the current batch of images concurrently.
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for img_path in batch:
                    blob = storage_client.bucket(bucket_name).blob(img_path)
                    local_image_path = os.path.join(input_dir, os.path.basename(img_path))
                    futures.append(executor.submit(blob.download_to_filename, local_image_path))
                # Ensure all downloads complete.
                for future in futures:
                    future.result()

            # Run inference on the batch.
            process_batch(model, input_dir, output_dir)

            # Upload annotated images back to GCS.
            annotated_files = [
                os.path.join(output_dir, f)
                for f in os.listdir(output_dir)
                if os.path.isfile(os.path.join(output_dir, f))
            ]
            if annotated_files:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = []
                    for annotated_file in annotated_files:
                        blob_name = os.path.join(annotated_gcs_path, os.path.basename(annotated_file))
                        blob = storage_client.bucket(bucket_name).blob(blob_name)
                        futures.append(executor.submit(blob.upload_from_filename, annotated_file))
                    for future in futures:
                        future.result()
                print(f"Uploaded {len(annotated_files)} annotated images to gs://{bucket_name}/{annotated_gcs_path}")

                # Clean up the output directory for the next batch.
                shutil.rmtree(output_dir)
                os.makedirs(output_dir, exist_ok=True)
            else:
                print("No annotated images to upload for this batch.")

            # Clean up the input directory for the next batch.
            shutil.rmtree(input_dir)
            os.makedirs(input_dir, exist_ok=True)

    print(f"Inference and upload completed successfully for folder: {folder_path}")


if __name__ == "__main__":

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        description="Batch image inference with YOLO and upload annotations to GCS."
    )
    parser.add_argument(
        "folders",
        type=str,
        help=("JSON dumped list of strings of folder paths in gs://BUCKET_NAME/ containing images/"
              "Example: '[\"1/2023-1-1/\"]'")
    )
    args = parser.parse_args()

    # The script expects a JSON string representing a list of folder paths.
    folder_paths = json.loads(args.folders)

    logging.info(f"baltimore-auto-annotation {folder_paths}: Inference script started")

    try: 
        for folder_path in folder_paths:
            main(folder_path)
    except Exception as e:
        logging.exception(f"baltimore-auto-annotation {folder_paths}: Inference script failed unexpectedly")
        raise

    logging.info(f"baltimore-auto-annotation {folder_paths}: Inference script succeeded")
