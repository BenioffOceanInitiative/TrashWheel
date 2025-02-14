# TrashWheel

# Semi-Automatic Annotation Pipeline

A pipeline for automating image annotation using a YOLO model and CVAT integration on Google Cloud Platform (GCP). This pipeline processes daily images captured from trash wheel devices, performs inference, and uploads the results for human validation.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Deployment](#deployment)
  - [Cloud Schedulers](#cloud-schedulers)
  - [Cloud Run Functions](#cloud-run-functions)
  - [Compute Engine Instances](#compute-engine-instances)
  - [Service Accounts](#service-accounts)
- [Components & Scripts](#components--scripts)
- [Redeployment & Updating](#redeployment--updating)
- [Required GCP Roles/Permissions](#required-gcp-rolespermissions)
- [Contributing](#contributing)
- [Future Work](#future-work)
- [License](#license)

---

## Overview

The **Semi-Automatic Annotation Pipeline** is designed to:
- Process images uploaded daily to a Cloud Storage bucket.
- Run inference on new images using a YOLO model.
- Upload auto-generated annotations to CVAT for human review.
- Leverage GCP services (Cloud Schedulers, Cloud Run, Compute Engine) for a scalable and cost-efficient solution.
<img width="464" alt="Screenshot 2025-02-14 at 11 04 26 AM" src="https://github.com/user-attachments/assets/2607a947-eab6-4e1c-b7a9-0fec66e17f62" />
<img width="157" alt="Screenshot 2025-02-14 at 11 04 42 AM" src="https://github.com/user-attachments/assets/52858fb9-f47f-435e-b189-58e2a434a957" />

---

## Architecture

The pipeline integrates several GCP components:

- **Cloud Schedulers** trigger Cloud Run functions daily.
- **Cloud Run Functions** manage folder validation, initiate Compute Engine instances, and handle annotation downloads.
- **Compute Engine Instances** run on-demand for image inference and CVAT uploads.
- **Service Accounts** provide necessary permissions for automated resource management.

> **Note:** An architecture diagram (created in Lucidchart) outlines the complete flow and is available in the repository.

---

## Deployment

### Cloud Schedulers

- **`baltimore-auto-annotation`**
  - **Schedule:** Every day at 9:30 AM.
  - **Action:** Pings the `baltimore-auto-annotation` Cloud Run function.

- **`baltimore-cvat-download`**
  - **Schedule:** Every day at 9:30 AM.
  - **Action:** Pings the `baltimore-cvat-download` Cloud Run function.

### Cloud Run Functions

- **`baltimore-auto-annotation`**
  - Scans for new daily folders following the schema:  
    `gs://trashwheel/images/{device_number}/YYYY-M-D`
  - Validates folders and skips those with existing annotations.
  - Initializes a Compute Engine instance (from the auto-annotation template) for inference.
  - **Runtime Variables:**  
    Specify device numbers, instance template name, bucket name, and CVAT API authentication.

- **`baltimore-cvat-download`**
  - Downloads new CVAT images in COCO format.
  - Uploads annotations to the bucket:  
    `gs://trashwheel/images/{device_number}/YYYY-M-D/annotations`
  - Ensures that already annotated folders are not re-uploaded.

### Compute Engine Instances

- **Instance Template:** `baltimore-auto-annotation`
  - **Machine Type:** n1-standard-8
  - **GPU:** 1× NVIDIA T4
  - **Timeouts:** Max duration of 2 hours with a short host error timeout.
  - **Cost:** Approximately \$1/day at max usage.

- **On-Demand Instances:** `baltimore-auto-annotation-YYYY-M-D`
  - **Workflow:**
    - Log status updates to Cloud Logging.
    - Download the most recent production model.
    - Download images from the specified Cloud Storage folders.
    - Run `inference.py` to generate annotations.
    - Run `upload_to_cvat.py` to upload images and annotations (YOLO 1.1 format) to CVAT.
    - Self-terminate and delete upon completion or error.

### Service Accounts

- **`baltimore-auto-annotation` Service Account**
  - Permissions include:
    - Create/delete VM instances.
    - Invoke Cloud Run functions.
    - Read/write access to Cloud Storage.

---

## Components & Scripts

- **VM Scripts (in the TrashWheel GitHub repository):**
  - `startup.sh`:  
    - Handles the entire VM lifecycle. Logs tasks, errors, and self-deletion events (prefixed with `[baltimore-auto-annotation-YYYY-M-D]`).
  - `inference.py`:  
    - Runs batched image inference and outputs annotation text files.
    - Supports adjustable confidence thresholds.
  - `upload_to_cvat.py`:  
    - Uploads images and annotation files to the CVAT API.

- **Cloud Run Function Scripts:**
  - `baltimore-auto-annotation.py` & `requirements.txt`
  - `baltimore-cvat-download` (includes `cvat_client.py` for CVAT interactions)

- **CVAT Utilities:**
  - `cvat_client.py`:  
    - Implements helper methods for CVAT interactions used by both Cloud Run and Compute Engine components.

> **Tip:** Both the inference and upload scripts are automatically pulled from the repository during VM initialization.

---

## Redeployment & Updating


**Manual Redeployment when Updating Cloud Run Functions or CVAT Client:**
- To update production:
  1. Manually update the Cloud Run functions on GCP.

**Planned Improvements:**
- Consider using GitHub Actions to automatically push changes to Cloud Run functions upon merging to the main branch.

**Manual Redeployment when Updating startup.sh:**
- To update production:
  1. Create a new VM instance template by selecting the current one and clicking **"Create Similar"**.
  2. In the **Management** section, update the startup script.
  3. Update the `INSTANCE_TEMPLATE_NAME` environment variable in the `baltimore-auto-annotation` Cloud Run function.

**Deployment for Other Script Updates:**
- Any other script change will be pulled automatically from the main branch, no deployment process needed.

**Planned Improvements:**
- Automate the provision of the instance template and runtime variables by fetching the latest `startup.sh` directly from the repository.

---

## Required GCP Roles/Permissions

Ensure the following roles are granted:
- `roles/cloudscheduler.admin`
- `roles/compute.viewer`
- `roles/compute.instanceAdmin.v1`
- `roles/run.invoker`
- `roles/iam.serviceAccountAdmin`
- `roles/storage.admin`

## Other Resources

1. Check the JIRA Ticket: https://boi-ucsb.atlassian.net/jira/software/projects/ENG/boards/3?selectedIssue=ENG-627
2. Check the comprehensive, internal documentation: https://boi-ucsb.atlassian.net/wiki/spaces/BE/pages/85393412/Semi-Automatic+Annotation+Pipeline+Management

---

## Contributing

Contributions are welcome! To contribute:

1. **Fork** the repository.
2. Create a **feature branch**: `git checkout -b feature/YourFeatureName`
3. **Commit** your changes: `git commit -m 'Add new feature'`
4. **Push** to the branch: `git push origin feature/YourFeatureName`
5. Open a **Pull Request** detailing your changes.

For major changes, please open an issue first to discuss what you would like to change.

---

## Future Work

- **Folder Validation:**  
  Use a `manifest.json` as the single source of truth for determining which folders need inference.
- **Secret Management:**  
  Integrate GCP Secret Manager for CVAT authentication and other sensitive environment variables.
- **Performance Optimization:**  
  Benchmark different instance types and refine timeout settings.
- **Scalability:**  
  Explore using Pub/Sub or Cloud Storage triggers if processing times exceed Cloud Run’s limits.
- **Documentation:**  
  Further document the `baltimore-cvat-download` function and associated workflows.

---

## License

This project is licensed under the MIT License.
