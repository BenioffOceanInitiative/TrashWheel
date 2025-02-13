import requests
from pathlib import Path
import shutil
import time
import zipfile
import os
from google.cloud import storage
from typing import Union

class CVATClient:
    def __init__(self, username=os.environ.get('CVAT_USERNAME'), password=os.environ.get('CVAT_PASSWORD'), org='BOSL', project_id=184108, bucket_mount_path='/trashwheel', class_names=[
        "plastic bottle", "polystyrene container", "food wrapper", 
        "polystyrene piece", "mini liquor bottle", "plastic bag",
        "plastic straw", "plastic toy", "ball", "plastic bottle cap",
        "plastic jug", "medicine bottle", "plastic cup", "plastic cup lid",
        "aluminum can", "plastic squeeze tube", "plastic container",
        "plastic utensil"
    ]):
        self.base_url = "https://app.cvat.ai/api"
        self.org = org
        self.project_id = project_id
        self.bucket_mount_path = Path(bucket_mount_path)
        self._tasks = None
        self._completed_tasks = None
        self.class_names = class_names
        self.session = requests.Session()
        
        # Authenticate immediately on init
        auth_response = self.session.post(
            f"{self.base_url}/auth/login",
            json={"username": username, "password": password}
        )
        
        if auth_response.status_code != 200:
            raise Exception(f"Authentication failed: {auth_response.status_code}")
            
        token = auth_response.json().get('key')
        if not token:
            raise Exception("No authentication token received")
            
        # Set token for all future requests
        self.session.headers.update({
            'Authorization': f'Token {token}',
            'Accept': 'application/vnd.cvat+json'
        })
        
    def upload_to_cvat(self, device_id: str, date: str) -> bool:
        """
        Upload data to CVAT
        
        Args:
            device_id: Device identifier
            date: Date string (YYYY-MM-DD)
            
        Returns:
            bool: True if upload succeeded
        """
        temp_dir = Path(os.path.dirname(os.path.abspath(__file__))) / f"temp_{device_id}_{date}"
        
        try:
            # Get data from mounted bucket
            self._get_device_data(device_id, date, temp_dir)
            
            # Create and populate task
            task_name = f"{device_id}_{date}"
            task = self._create_task(task_name)
            
            # Upload images and annotations
            images_path = temp_dir / device_id / date / 'images'
            self._upload_images(task['id'], images_path)
            
            zip_file = self._prepare_yolo_data(temp_dir)
            self._upload_annotations(task['id'], zip_file)
            
            zip_file.unlink()
            print(f"Upload complete for Task: {task_name}")
            return True
            
        except Exception as e:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            print(f"Upload failed: {e}")
            return False
            
        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    def export_annotations(self, device_id: str, date: str, cloud_storage_id: int = 2140) -> bool:
        """
        Export CVAT annotations to Google Cloud Storage.
        
        Args:
            device_id: The device ID (e.g., "3")
            date: The date string (e.g., "2025-1-3")
            cloud_storage_id: The CVAT cloud storage ID (default: 2140 for trashwheel bucket)
            
        Returns:
            bool: True if export succeeded, False otherwise
        """
        try:
            if self._completed_tasks is None:
                self._completed_tasks = self.get_completed_tasks(force_refresh=True)
            
            task_name = f"{device_id}_{date}"
            storage_path = f"{device_id}/{date}"
            
            task = self.get_completed_task(name = task_name)
            
            # Set up cloud storage path
            self.session.patch(
                f"{self.base_url}/cloudstorages/{cloud_storage_id}",
                json={"prefix": storage_path}
            )
            
            # Export annotations
            export_params = {
                "format": "COCO 1.0",
                "location": "cloud_storage",
                "cloud_storage_id": cloud_storage_id,
                "filename": f"{storage_path}/annotations.zip",
                "use_default_location": False
            }
            
            export_response = self.session.get(
                f"{self.base_url}/tasks/{task['id']}/annotations",
                params=export_params
            )
            
            if export_response.status_code == 202:  # Async processing
                rq_id = export_response.json().get('rq_id')
                if not rq_id:
                    raise Exception("No request ID received")
                
                print(f"Export initiated with request ID: {rq_id}")
                
                # Wait for completion
                for attempt in range(60):  # 10 minutes timeout
                    time.sleep(10)
                    status_response = self.session.get(
                        f"{self.base_url}/tasks/{task['id']}/annotations",
                        params={**export_params, "rq_id": rq_id}
                    )
                    
                    if status_response.status_code in [200, 201]:
                        print(f"Successfully exported to gs://trashwheel/{storage_path}/annotations.zip")
                        return True
                    elif status_response.status_code != 202:
                        raise Exception(f"Export failed: {status_response.status_code}")
                    
                    print(f"Export in progress... (Attempt {attempt + 1}/60)")
                
                raise Exception("Export timed out")
                
            elif export_response.status_code in [200, 201]:
                print(f"Successfully exported to gs://trashwheel/{storage_path}/annotations.zip")
                return True
            else:
                raise Exception(f"Export failed: {export_response.status_code}")
                
        except Exception as e:
            print(f"Export failed: {e}")
            return False
        
    def _get_task_status(self, task_id: int) -> str:
        """Get the current status of a task"""
        response = self.session.get(
            f"{self.base_url}/tasks/{task_id}/status",
            headers={'Accept': 'application/vnd.cvat+json'}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get task status: {response.status_code}")
        
        status_data = response.json()
        return status_data.get('state', 'Unknown')
            
    def _fetch_all_tasks(self):
        """Internal method to fetch all tasks with pagination handling"""
        all_tasks = []
        next_url = f"{self.base_url}/tasks"
        
        while next_url:
            response = self.session.get(next_url)
            if response.status_code != 200:
                raise Exception(f"Failed to get tasks: {response.status_code}")
                
            data = response.json()
            all_tasks.extend(data['results'])
            next_url = data.get('next')
            
        return all_tasks
    
    def get_all_tasks(self, force_refresh=False):
        """
        Get all tasks with caching
        
        Args:
            force_refresh: If True, force a refresh of the task cache
            
        Returns:
            list: All tasks
        """
        if self._tasks is None or force_refresh:
            self._tasks = self._fetch_all_tasks()
        
        if self._tasks is None:
            raise Exception(f"No tasks found for project: {self.project_id}")
        
        return self._tasks
    
    def get_task(self, name: str):
        """Get task details by name"""
        task = next((t for t in self._tasks if t['name'] == name), None)
        
        if not task:
            raise Exception(f"Task '{task_name}' not found")
            
        return task
    
    def get_completed_tasks(self, force_refresh=False):
        """
        Get all completed tasks
        
        Args:
            force_refresh: If True, force a refresh of the task cache
            
        Returns:
            list: List of completed tasks sorted by name
        """
        tasks = self.get_all_tasks(force_refresh=force_refresh)
        self._completed_tasks = [t for t in tasks if t['status'] == 'completed']
        # Sort by name for easier reading
        self._completed_tasks.sort(key=lambda x: x['name'])
        
        if self._completed_tasks is None:
            raise Exception(f"No completed tasks found for project: {self.project_id}")
        return self._completed_tasks
    
    def get_completed_task(self, name: str) -> dict:
        """Get completed task by name"""
        completed_task = next((t for t in self._completed_tasks if t['name'] == name), None)
        if not completed_task:
            if not self.get_task(name):
                raise Exception(f"Task '{name}' was not found")
            else:
                raise Exception(f"Task '{name}' is not completed")
        print(f"Found completed task: {completed_task}")
        return completed_task

    def _get_device_data(self, device_id: str, date: str, temp_dir: Path) -> None:
        """
        Copy images and annotations from mounted bucket to temporary directory
        """
        # Source paths
        device_path = self.bucket_mount_path / device_id / date
        images_src = device_path / 'images'
        print(f"Looking for images in: {images_src}")
        
        if not device_path.exists():
            raise Exception(f"Source directory not found at {device_path}")
        
        if not images_src.exists():
            raise Exception(f"Images directory not found at {images_src}")
        
        # Create temporary directories with device/date/images structure
        images_dest = temp_dir / device_id / date / 'images'  # Add 'images' to the path
        annotations_dest = temp_dir / 'annotations'  # Keep annotations separate
        images_dest.mkdir(parents=True, exist_ok=True)
        annotations_dest.mkdir(parents=True, exist_ok=True)
        
        # Copy images maintaining device/date/images structure
        image_count = 0
        for pattern in ['*.[jJ][pP][gG]', '*.[jJ][pP][eE][gG]', '*.[pP][nN][gG]']:
            for img in images_src.glob(pattern):
                shutil.copy2(img, images_dest / img.name)
                image_count += 1
        
        print(f"Total images found: {image_count}")
        
        if image_count == 0:
            raise Exception(f"No images found in {images_src}")
            
        # Copy annotations (assuming they're in auto-annotations subdirectory)
        annotations_src = device_path / 'auto-annotations'
        if not annotations_src.exists():
            raise Exception(f"Annotations directory not found at {annotations_src}")
            
        annotation_count = 0
        for annotation in annotations_src.glob('*.txt'):
            shutil.copy2(annotation, annotations_dest / annotation.name)
            annotation_count += 1
        
        if annotation_count == 0:
            raise Exception(f"No annotations found in {annotations_src}")

    def _create_task(self, name: str) -> dict:
        """
        Create a new CVAT task
        
        Args:
            name: Name of the task
            
        Returns:
            dict: Task details including ID
            
        Raises:
            Exception: If task creation fails or response is invalid
        """
        # Include organization in request
        headers = {'X-Organization': self.org} if self.org else {}
        
        response = self.session.post(
            f"{self.base_url}/tasks",
            json={
                "name": name,
                "project_id": self.project_id,
                "organization": self.org
            },
            headers=headers  # Add organization header
        )
        
        if response.status_code != 201:
            raise Exception(f"Failed to create task: {response.status_code} - {response.text}")
            
        task_data = response.json()
        
        # Verify we got a valid task response with an ID
        if not task_data or 'id' not in task_data:
            raise Exception(f"Invalid task creation response: {task_data}")
            
        print(f"Created task with ID: {task_data['id']} and name: {task_data['name']}")
        return task_data
    
    def _upload_images(self, task_id: int, image_dir: Union[str, Path]) -> None:
        """
        Upload images to a task, maintaining directory structure
        
        Args:
            task_id: CVAT task ID
            image_dir: Path to directory containing images
            
        Raises:
            Exception: If upload fails or task processing fails
        """
        image_dir = Path(image_dir)
        zip_file = self._prepare_image_zip(image_dir)
        
        try:
            with open(zip_file, 'rb') as f:
                upload_response = self.session.post(
                    f"{self.base_url}/tasks/{task_id}/data",
                    files={'client_files[0]': f},
                    data={
                        'image_quality': 70,
                        'use_zip_chunks': True,
                        'use_cache': True
                    }
                )
                if upload_response.status_code not in [201, 202]:
                    raise Exception(f"Failed to upload images: {upload_response.status_code} - {upload_response.text}")

            print("Waiting for image processing...")
            while True:
                status = self._get_task_status(task_id)
                if status == "Failed":
                    raise Exception("Task processing failed")
                if status == "Finished":
                    break
                time.sleep(15)
            print("Image upload complete")
        finally:
            zip_file.unlink()

    def _prepare_image_zip(self, image_dir: Union[str, Path]) -> Path:
        """
        Create a zip file containing images with full directory structure
        
        Args:
            image_dir: Path to directory containing images
            
        Returns:
            Path: Path to created zip file
            
        Raises:
            Exception: If no images found in directory
        """
        image_dir = Path(image_dir)
        temp_dir = Path("temp_images")
        temp_dir.mkdir(exist_ok=True, parents=True)
        
        # Get device_id and date from path
        # image_dir is like ".../temp_3_2025-1-4/3/2025-1-4/images"
        device_id = image_dir.parent.parent.name
        date = image_dir.parent.name
        
        # Create full directory structure
        dest_dir = temp_dir / device_id / date / 'images'
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        image_count = 0
        for pattern in ["*.[pP][nN][gG]", "*.[jJ][pP][gG]", "*.[jJ][pP][eE][gG]"]:
            for img_file in image_dir.glob(pattern):
                # Copy maintaining full path structure
                shutil.copy2(img_file, dest_dir / img_file.name)
                image_count += 1
        
        if image_count == 0:
            raise Exception(f"No images found in {image_dir}")
        
        zip_path = Path("images.zip")
        if zip_path.exists():
            zip_path.unlink()
            
        # Create zip from the parent directory to include full structure
        shutil.make_archive(zip_path.stem, 'zip', temp_dir)
        
        shutil.rmtree(temp_dir)
        return zip_path

    def _prepare_yolo_data(self, data_dir: Path) -> Path:
        """
        Prepare YOLO format data for upload
        
        Args:
            data_dir: Base directory containing device/date structure
            
        Returns:
            Path: Path to created zip file
            
        Raises:
            Exception: If no images found or annotation creation fails
        """
        temp_dir = Path("temp_upload")
        temp_dir.mkdir(exist_ok=True, parents=True)
        train_dir = temp_dir / "obj_train_data"
        train_dir.mkdir(exist_ok=True)
        
        try:
            train_txt = []
            image_count = 0
            
            # Recursively find all images in the device/date structure
            for pattern in ["*.[pP][nN][gG]", "*.[jJ][pP][gG]", "*.[jJ][pP][eE][gG]"]:
                for img_file in data_dir.rglob(pattern):
                    # Get relative path components
                    rel_path = img_file.relative_to(data_dir)
                    
                    # Create corresponding directory in train_dir if needed
                    dest_dir = train_dir / rel_path.parent
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Copy image maintaining directory structure
                    shutil.copy2(img_file, dest_dir / img_file.name)
                    
                    # Add relative path to train.txt
                    train_txt.append(f"obj_train_data/{rel_path}")
                    image_count += 1
                    
                    # Copy corresponding annotation if exists
                    anno_file = data_dir / 'annotations' / f"{img_file.stem}.txt"
                    if anno_file.exists():
                        shutil.copy2(anno_file, dest_dir / f"{img_file.stem}.txt")
            
            if image_count == 0:
                raise Exception(f"No images found in {data_dir}")
            
            # Create supporting files
            with open(temp_dir / "obj.names", "w") as f:
                f.write("\n".join(self.class_names))

            with open(temp_dir / "obj.data", "w") as f:
                f.write(f"classes = {len(self.class_names)}\n")
                f.write("names = obj.names\n")
                f.write("train = train.txt\n")
            
            with open(temp_dir / "train.txt", "w") as f:
                f.write("\n".join(train_txt))
            
            zip_path = Path("upload.zip")
            if zip_path.exists():
                zip_path.unlink()
            shutil.make_archive(zip_path.stem, 'zip', temp_dir)
            
            return zip_path
            
        finally:
            shutil.rmtree(temp_dir)

    def _upload_annotations(self, task_id: int, zip_file: Path) -> None:
        """
        Upload YOLO annotations to a task
        
        Raises:
            Exception: If upload fails or annotation processing fails
        """
        print(f"Uploading YOLO annotations to task {task_id}")
        with open(zip_file, 'rb') as f:
            upload_response = self.session.put(
                f"{self.base_url}/tasks/{task_id}/annotations?format=YOLO 1.1",
                files={'annotation_file': f},
                headers={'Accept': 'application/vnd.cvat+json'}
            )
            if upload_response.status_code not in [200, 202]:
                raise Exception(f"Failed to upload annotations: {upload_response.status_code} - {upload_response.text}")
        
        if upload_response.status_code == 202:
            print("Waiting for annotation processing...")
            while True:
                status = self._get_task_status(task_id)
                if status == "Failed":
                    raise Exception("Annotation processing failed")
                if status == "Finished":
                    break
                time.sleep(5)
            print("Annotation upload complete")
