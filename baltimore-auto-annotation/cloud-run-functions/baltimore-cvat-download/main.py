import functions_framework
from google.cloud import storage
import zipfile
import tempfile
import os
from cvat_client import CVATClient
import time
from flask import jsonify
import json

MANIFEST_PATH = 'scripts/cloud_functions/downloaded_cvat_annotations_manifest.json'

def folder_exists(bucket, prefix):
    """Check if a folder exists by looking for any objects with the given prefix"""
    blobs = list(bucket.list_blobs(prefix=prefix, delimiter='/', max_results=1))
    prefixes = list(bucket.list_blobs(prefix=prefix, delimiter='/').prefixes)
    return len(blobs) > 0 or len(prefixes) > 0

def load_processed_manifest(bucket):
    """Load the manifest of processed folders, create if not exists"""
    manifest_blob = bucket.blob(MANIFEST_PATH)
    if manifest_blob.exists():
        content = manifest_blob.download_as_string()
        return json.loads(content)
    else:
        print(f"Manifest not found at {MANIFEST_PATH}, creating new one")
        # Create an empty manifest
        empty_manifest = {}
        manifest_blob.upload_from_string(json.dumps(empty_manifest, indent=2))
        return empty_manifest

def update_processed_manifest(bucket, device_id, date, status="completed"):
    """Update the manifest with newly processed folder"""
    manifest_blob = bucket.blob(MANIFEST_PATH)
    
    if manifest_blob.exists():
        content = manifest_blob.download_as_string()
        manifest = json.loads(content)
    else:
        manifest = {}
    
    if device_id not in manifest:
        manifest[device_id] = {}
    
    manifest[device_id][date] = {
        "status": status,
        "processed_at": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    manifest_blob.upload_from_string(json.dumps(manifest, indent=2))

@functions_framework.http
def cvat_download(request):
    """Cloud Function to process trashwheel bucket folders and manage annotations"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket('trashwheel')
        client = CVATClient()

        # Load the manifest of previously processed folders
        processed_folders = load_processed_manifest(bucket)
        print(f"Loaded manifest: found {sum(len(dates) for dates in processed_folders.values())} processed items")
        
        results = []

        for device_id in ['1', '2', '3']:
            device_id = str(device_id)  # Ensure string type
            print(f"Processing device ID: {device_id}")
            
            date_prefix = f"{device_id}/"
            blobs = bucket.list_blobs(prefix=date_prefix, delimiter='/')
            
            date_folders = set()
            for page in blobs.pages:
                for prefix in page.prefixes:
                    date = prefix.split('/')[1]
                    date_folders.add(date)

            for date in date_folders:
                # Skip if already successfully processed
                if (device_id in processed_folders and 
                    date in processed_folders[device_id] and 
                    processed_folders[device_id][date].get("status") == "completed"):
                    print(f"Skipping already processed folder: Device {device_id}, Date {date}")
                    continue
                
                auto_annotations_path = f"{device_id}/{date}/auto-annotations/"
                annotations_path = f"{device_id}/{date}/annotations/"
                
                auto_annotations_exists = folder_exists(bucket, auto_annotations_path)
                annotations_exists = folder_exists(bucket, annotations_path)

                print(f"Device {device_id}, Date {date}: Has auto-annotations: {auto_annotations_exists}, Has annotations: {annotations_exists}")

                if auto_annotations_exists and not annotations_exists:
                    print(f"Found candidate folder: Device {device_id}, Date {date}")
                    
                    try:
                        export_success = client.export_annotations(device_id, date)
                        
                        if export_success:
                            print(f"CVAT exported initiated successfully for Device {device_id}, Date {date}")
                            
                            max_wait = 300  # 5 minutes timeout
                            start_time = time.time()
                            zip_path = f"{device_id}/{date}/annotations.zip"
                            print(f"Waiting for zip file to appear... Start Epoch Time: start_time")
                            while time.time() - start_time < max_wait:
                                if bucket.blob(zip_path).exists():
                                    print(f"Zip file received for Device {device_id}, Date {date}")
                                    try:
                                        process_zip_file(bucket, zip_path)
                                        # Update manifest after successful processing
                                        update_processed_manifest(bucket, device_id, date, "completed")
                                        results.append({
                                            "device_id": device_id,
                                            "date": date,
                                            "status": "success"
                                        })
                                        print(f"Successfully processed zip file for Device {device_id}, Date {date}, ({int(time.time() - start_time)}s elapsed")
                                        break
                                    except Exception as zip_error:
                                        print(f"Error processing zip file: {str(zip_error)}")
                                        update_processed_manifest(bucket, device_id, date, "failed_zip_processing")
                                        results.append({
                                            "device_id": device_id,
                                            "date": date,
                                            "status": "zip_processing_error",
                                            "error": str(zip_error)
                                        })
                                        break
                                time.sleep(10)
                            else:
                                print(f"Timeout waiting for zip file: Device {device_id}, Date {date}")
                                update_processed_manifest(bucket, device_id, date, "timeout_waiting_for_zip")
                                results.append({
                                    "device_id": device_id,
                                    "date": date,
                                    "status": "timeout_waiting_for_zip"
                                })
                        else:
                            print(f"Export failed for Device {device_id}, Date {date}")
                            update_processed_manifest(bucket, device_id, date, "export_failed")
                            results.append({
                                "device_id": device_id,
                                "date": date,
                                "status": "export_failed"
                            })
                    
                    except Exception as e:
                        print(f"Error processing Device {device_id}, Date {date}: {str(e)}")
                        update_processed_manifest(bucket, device_id, date, "error")
                        results.append({
                            "device_id": device_id,
                            "date": date,
                            "status": "error",
                            "error": str(e)
                        })
                    
                    time.sleep(5)

        return jsonify({
            "status": "completed",
            "processed_items": results
        })

    except Exception as e:
        print(f"Global error in function: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

def process_zip_file(bucket, zip_path):
    """Process a zip file in the bucket: download, extract, and delete original"""
    print(f"Starting to process zip file: {zip_path}")
    with tempfile.TemporaryDirectory() as temp_dir:
        local_zip = os.path.join(temp_dir, "annotations.zip")
        blob = bucket.blob(zip_path)
        print(f"Downloading zip file to {local_zip}")
        blob.download_to_filename(local_zip)

        try:
            print(f"Extracting zip file")
            with zipfile.ZipFile(local_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            base_dir = os.path.dirname(zip_path)

            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file == "annotations.zip":
                        continue
                        
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, temp_dir)
                    destination_path = f"{base_dir}/{relative_path}"
                    
                    print(f"Uploading {relative_path} to {destination_path}")
                    blob = bucket.blob(destination_path)
                    blob.upload_from_filename(local_path)

            print(f"Deleting original zip file")
            blob = bucket.blob(zip_path)
            blob.delete()
            print(f"Successfully processed zip file: {zip_path}")

        except Exception as e:
            print(f"Error processing zip file {zip_path}: {str(e)}")
            raise e