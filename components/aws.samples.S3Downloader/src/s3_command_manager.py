import asyncio
import json
import re
import os
import uuid
import time
from typing import Dict, Any, Optional, List

from .utils.logging_config import get_logger
from .s5cmd_async import AsyncS5CommandController

logger = get_logger(__name__)


class S3CommandManager:
    """
    Manages S3 commands execution and tracking using the AsyncS5CommandController
    """
    def __init__(self, device_id: str, default_download_dir: str = "./downloads"):
        self.device_id = device_id
        self.default_download_dir = default_download_dir
        # Ensure the download directory exists
        os.makedirs(default_download_dir, exist_ok=True)
        
        # Store active downloads with their controllers and metadata
        self.active_downloads: Dict[str, Dict[str, Any]] = {}
        self.controller = AsyncS5CommandController()
        
    async def execute_command(self, command_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a command based on the provided command dictionary
        
        Args:
            command_dict: Command parameters
                {
                    'command': 'download',              # Required: Command type
                    'bucket': 'my-bucket',              # Required for download: S3 bucket name
                    'key': 'my-file.txt',               # Required for download: S3 object key
                    'destination': '/path',             # Optional: Download destination path
                    'numworkers': 256                   # Optional: Size of the global worker pool
                    'concurrency': 5                    # Optional: Number of parts that will be uploaded or downloaded in parallel for a single file
                    'retry_count': 10                   # Optional: Retry count for up to a minute
                    's3_transfer_acceleration': False   # Optional: Use S3 transfer acceleration
                    'command_id': 'abc123'               # Optional: Client-provided command ID
                }
                
        Returns:
            Dictionary with operation result and metadata
            {
                'success': True,     # Whether the operation succeeded
                'download_id': '...', # For download commands
                'message': '...',    # Operation result message
                'status': '...'      # Current status
            }
        """
        logger.info(f"Executing command: {json.dumps(command_dict)}")
        command_type = command_dict.get('command', '').lower()
        
        if not command_type:
            return {'success': False, 'error': 'Missing command type'}
            
        if command_type == 'download':
            return await self._handle_download(command_dict)
        elif command_type == 'list':
            return await self._handle_list(command_dict)
        elif command_type in ['pause', 'resume', 'cancel']:
            return await self._handle_control_command(command_type, command_dict)
        elif command_type == 'getdetails':
            download_id = command_dict.get('download_id')
            if not download_id or download_id not in self.active_downloads:
                return {'success': False, 'error': f'Download ID {download_id} not found'}
            
            download_info = self.active_downloads[download_id]
            
            # Create a sanitized copy of download info for response
            details = {
                'download_id': download_id,
                'bucket': download_info.get('bucket'),
                'key': download_info.get('key'),
                'destination': download_info.get('destination'),
                'status': download_info.get('status'),
                'progress': download_info.get('progress', 0),
                'start_time': download_info.get('start_time'),
                'end_time': download_info.get('end_time'),
                'error_details': download_info.get('error_details')
            }
            
            return {
                'success': True,
                'details': details
            }
        elif command_type == 'disk-space':
            # Check disk space
            disk_info = self.check_disk_space()
            return {
                'success': True,
                'disk_space': {
                    'total_gb': round(disk_info['total_gb'], 2),
                    'used_gb': round(disk_info['used_gb'], 2),
                    'free_gb': round(disk_info['free_gb'], 2),
                    'percent_used': round(disk_info['used_gb'] / disk_info['total_gb'] * 100, 2)
                }
            }
        elif command_type == 'status':
            # Handle the status command
            # Get active downloads
            active_downloads = []
            for download_id, info in self.active_downloads.items():
                # Only include active downloads
                if info.get('status') in ['downloading', 'paused']:
                    active_downloads.append({
                        'download_id': download_id,
                        'bucket': info.get('bucket'),
                        'key': info.get('key'),
                        'status': info.get('status'),
                        'progress': float(info.get('progress', 0)),  # Ensure it's a float
                        'destination': info.get('destination'),
                        'start_time': info.get('start_time')
                    })
            
            # Get disk space info
            disk_info = self.check_disk_space()
            
            # Return the status - maintaining consistent structure with other commands
            return {
                'success': True,
                'command_id': command_dict.get('command_id'),  # Return the command ID if provided
                'status': 'completed',
                'message': 'System status retrieved successfully',
                'system_info': {
                    'active_downloads': len(active_downloads),
                    'downloads': active_downloads,
                    'disk_space': {
                        'total_gb': round(disk_info['total_gb'], 2),
                        'used_gb': round(disk_info['used_gb'], 2),
                        'free_gb': round(disk_info['free_gb'], 2),
                        'percent_used': round(disk_info['used_gb'] / disk_info['total_gb'] * 100, 2)
                    }
                }
            }
        else:
            return {'success': False, 'error': f'Unknown command type: {command_type}'}
    
    async def _handle_download(self, command_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Handle download command"""
        # Extract parameters
        bucket = command_dict.get('bucket')
        key = command_dict.get('key')
        command_id = command_dict.get('command_id')
        if not command_id:
            # Generate a fallback command ID if none was provided
            command_id = f"auto-{str(uuid.uuid4())[:8]}"
            logger.warning(f"No command_id provided, generated fallback: {command_id}")
        
        # Log the full command dictionary to see what's incoming
        logger.info(f"Download command received: {json.dumps(command_dict)}")
        logger.info(f"Command ID received: {command_id}")
                
        if not bucket or not key:
            return {'success': False, 'error': 'Missing required parameters: bucket and key'}
        
        # Generate a download ID if not provided
        download_id = command_dict.get('download_id', str(uuid.uuid4()))
        
        # If there's already an active download with this ID, return an error
        if download_id in self.active_downloads:
            return {'success': False, 'error': f'Download with ID {download_id} already exists'}
        
        # Determine destination path
        destination = command_dict.get('destination', self.default_download_dir)
        os.makedirs(destination, exist_ok=True)
        
        # Extract other parameters with defaults
        numworkers = command_dict.get('numworkers', 256)
        concurrency = command_dict.get('concurrency', 5)
        retry_count = command_dict.get('retry_count', 10)
        s3_transfer_acceleration = command_dict.get('s3_transfer_acceleration', False)
        download_timeout = command_dict.get('download_timeout', 3000)  # 50 minutes default

        # Check and process model metadata if present
        model_meta = command_dict.get('model_meta')
        if model_meta:
            # If model_id is not provided, generate one based on the filename
            if 'model_id' not in model_meta:
                filename = os.path.basename(key.rstrip('/'))
                model_id = os.path.splitext(filename)[0]  # Remove extension
                # If still empty, use a generic name
                if not model_id:
                    model_id = f"model-{str(uuid.uuid4())[:8]}"
                    
                model_meta['model_id'] = model_id
                
            # Log model metadata
            logger.info(f"Model metadata included in download command: {json.dumps(model_meta)}")
                
        # Build the s5cmd command
        s5cmd_args = []
         
        # Add command and its parameters
        s5cmd_args.append("cp")
        s5cmd_args.extend(["--concurrency", str(concurrency)])
        s5cmd_args.extend(["--show-progress"])
        
        # Source and destination - handle directory downloads correctly
        if key.endswith('/'):
            s3_path = f"s3://{bucket}/{key}*"  # Add wildcard for directories
        else:
            s3_path = f"s3://{bucket}/{key}"
        
        s5cmd_args.extend([s3_path, destination])

        # Store global options separately
        global_options = []
        global_options.extend(["--numworkers", str(numworkers)])
        global_options.extend(["--retry-count", str(retry_count)])
        
        if s3_transfer_acceleration:
            global_options.append("--use-accelerate-endpoint")

        # Create an entry for this download
        download_info = {
            'id': download_id,
            'bucket': bucket,
            'key': key,
            'destination': destination,
            'start_time': time.time(),
            'status': 'starting',
            'progress': 0,
            'controller': self.controller,
            's5cmd_args': s5cmd_args,
            'global_options': global_options,
            'file_name': os.path.basename(key.rstrip('/')),
            'task': None,
            'command_id': command_id
        }

        # Store model metadata if provided
        if model_meta:
            download_info['model_meta'] = model_meta
            
        # Log the download info to verify command_id is stored
        logger.info(f"Created download info with command_id: {download_info.get('command_id')}")
        logger.info(f"Download info keys: {list(download_info.keys())}")

        self.active_downloads[download_id] = download_info
        
        # Create a task for the download that will manage its execution and completion
        download_task = asyncio.create_task(
            self._execute_download(download_id, s5cmd_args, global_options, download_timeout)
        )
        download_info['task'] = download_task
        download_info['status'] = 'downloading'
        
        return {
            'success': True,
            'download_id': download_id,
            'message': f'Download started with ID: {download_id}',
            'status': 'downloading'
        }
    
    async def _execute_download(self, download_id: str, s5cmd_args: List[str], global_options: List[str], timeout: int) -> None:
        """
        Execute the download operation and handle completion
        
        Args:
            download_id: The ID of the download
            s5cmd_args: Arguments for s5cmd
            global_options: Arguments for global_options of s5cmd
            timeout: Timeout in seconds
        """
        download_info = self.active_downloads.get(download_id)
        if not download_info:
            logger.error(f"Download ID {download_id} not found in active downloads")
            return
        
        logger.info(f"Starting download {download_id}: s5cmd {' '.join(global_options + s5cmd_args)}")
        
        # Store first and last error message
        error_messages = []
        last_progress_update = time.time()
        
        try:
            # Define callback for real-time progress updates and error capture
            async def progress_callback(output_type, line):
                nonlocal last_progress_update
                
                if download_id not in self.active_downloads:
                    return
                    
                if output_type == "stderr":
                    # Try to extract progress percentage using regex
                    progress_pattern = r'(\d+\.\d+)%'
                    matches = re.findall(progress_pattern, line)
                    
                    if matches:
                        # Get the last match (most recent percentage)
                        try:
                            progress = float(matches[-1])  # Use the last percentage found in the line
                            current_progress = self.active_downloads[download_id].get('progress', 0)
                            if progress > current_progress:
                                self.active_downloads[download_id]['progress'] = progress
                                self.active_downloads[download_id]['last_progress_update'] = time.time()
                                last_progress_update = time.time()
                                logger.info(f"Download {download_id} progress: {progress}%")
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Error parsing progress from line: {line}, error: {e}")
                    
                    # Check for specific error messages
                    if "no space left on device" in line.lower():
                        # Preserve current progress when error occurs
                        current_progress = self.active_downloads[download_id].get('progress', 0)
                        error_message = f"No space left on device at {current_progress}% progress"
                        logger.error(f"Download {download_id} failed: {error_message}")
                        
                        error_messages.append(error_message)
                        self.active_downloads[download_id]['status'] = 'failed'
                        self.active_downloads[download_id]['error_details'] = error_message
                        self.active_downloads[download_id]['end_time'] = time.time()
                        
                        # Publish error notification directly
                        await self.publish_error_notification(
                            download_id,
                            "No space left on device",
                            error_message
                        )
                        
                        # Try to cancel the download
                        await self.controller.cancel()
                        
                    # Store stderr lines as potential error messages
                    elif line.strip():
                        error_messages.append(line.strip())
            
            # Start the download with a timeout
            try:
                result = await asyncio.wait_for(
                    self.controller.execute_and_wait(s5cmd_args, global_options, callback=progress_callback),
                    timeout=timeout
                )
                
                # Update download info with result
                if download_id in self.active_downloads:
                    download_info = self.active_downloads[download_id]
                    previous_status = download_info.get('status')
                    
                    # Handle unsuccessful downloads explicitly
                    if not result['success']:
                        error_msg = "Download failed with s5cmd error"
                        download_info['status'] = 'failed'
                        download_info['end_time'] = time.time()
                        download_info['result'] = result
                        
                        # Get error details from collected stderr messages
                        if error_messages:
                            if len(error_messages) > 5:
                                detailed_msg = '\n'.join(error_messages[-5:])
                            else:
                                detailed_msg = '\n'.join(error_messages)
                        else:
                            detailed_msg = f"Return code: {result.get('return_code')}, State: {result.get('state')}"
                        
                        download_info['error_details'] = detailed_msg
                        logger.error(f"Download {download_id} failed: {error_msg}")
                        logger.error(f"Error details: {detailed_msg[:100]}...")
                        
                        # Publish error notification directly
                        await self.publish_error_notification(download_id, error_msg, detailed_msg)
                    else:
                        # Handle successful downloads
                        download_info['status'] = 'completed'
                        download_info['end_time'] = time.time()
                        download_info['result'] = result
                        logger.info(f"Download {download_id} completed successfully")
                        
                        # Publish success notification
                        await self.publish_completion_notification(download_id, True)
                
            except asyncio.TimeoutError:
                # Handle timeout
                timeout_msg = f"Download timed out after {timeout} seconds"
                logger.error(f"Download {download_id} {timeout_msg}")
                
                if download_id in self.active_downloads:
                    self.active_downloads[download_id]['status'] = 'timeout'
                    self.active_downloads[download_id]['error_details'] = timeout_msg
                    self.active_downloads[download_id]['end_time'] = time.time()
                    
                    # Include any collected error messages
                    if error_messages:
                        detailed_msg = f"{timeout_msg}. Last errors:\n" + '\n'.join(error_messages[-3:])
                    else:
                        detailed_msg = timeout_msg
                    
                    # Publish error notification for timeout
                    await self.publish_error_notification(download_id, "Download timed out", detailed_msg)
                    
                    # Try to cancel the download
                    await self.controller.cancel()
                    
        except Exception as e:
            # Handle any other errors during download execution
            error_msg = f"Error in download execution: {str(e)}"
            logger.error(f"Download {download_id} failed: {error_msg}")
            
            if download_id in self.active_downloads:
                self.active_downloads[download_id]['status'] = 'error'
                self.active_downloads[download_id]['error'] = str(e)
                self.active_downloads[download_id]['end_time'] = time.time()
                
                # Include traceback in error details
                import traceback
                error_details = f"{error_msg}\n{traceback.format_exc()}"
                self.active_downloads[download_id]['error_details'] = error_details
                
                # Publish error notification
                await self.publish_error_notification(download_id, error_msg, error_details)

    async def publish_error_notification(self, download_id: str, error_message: str, error_details: str = None):
        """
        Directly publish an error notification for a download
        
        Args:
            download_id: The ID of the download
            error_message: Brief error message
            error_details: Optional detailed error information
        """
        download_info = self.active_downloads.get(download_id)
        if not download_info:
            return

        # Get the command ID from download info - with debug logging
        command_id = download_info.get('command_id')
        logger.info(f"Creating error notification for download {download_id}, command_id: {command_id}")
        logger.info(f"Download info keys: {list(download_info.keys())}")
                    
        # Create an error notification event
        notification = {
            'event': 'download_error',
            'download_id': download_id,
            'success': False,
            'status': 'failed',
            'error': error_message,
            'progress': download_info.get('progress', 0),
            'command_id': command_id,
            'bucket': download_info.get('bucket'),
            'key': download_info.get('key')
        }
        
        if error_details:
            if len(error_details) > 256:
                notification['error_details'] = "..." + error_details[-256:]
            else:
                notification['error_details'] = error_details
        
        # Signal to the parent service to publish this notification
        download_info['error_notification'] = notification
        logger.info(f"Created error notification for download {download_id} with command_id: {command_id}")

    async def publish_completion_notification(self, download_id: str, success: bool):
        """
        Publish a completion notification for a download
        
        Args:
            download_id: The ID of the download
            success: Whether the download succeeded
        """
        download_info = self.active_downloads.get(download_id, {})
        
        notification = {
            'event': 'download_completed',
            'download_id': download_id,
            'success': success,
            'status': download_info.get('status', 'unknown'),
            'progress': download_info.get('progress', 0),
            'bucket': download_info.get('bucket'),
            'key': download_info.get('key'),
            'destination': download_info.get('destination'),
            'duration': time.time() - download_info.get('start_time', time.time()),
            'command_id': download_info.get('command_id')
        }
        
        # Signal to the parent service to publish this notification
        download_info['completion_notification'] = notification
        logger.info(f"Created completion notification for download {download_id}")
        
    async def _handle_list(self, command_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Handle list command for S3 objects or active downloads"""
        list_type = command_dict.get('listType', 'downloads')
        
        if list_type == 'downloads':
            # List active downloads
            downloads_list = []
            for download_id, info in self.active_downloads.items():
                downloads_list.append({
                    'download_id': download_id,
                    'bucket': info.get('bucket'),
                    'key': info.get('key'),
                    'status': info.get('status'),
                    'progress': info.get('progress', 0),
                    'destination': info.get('destination'),
                    'start_time': info.get('start_time')
                })
            return {
                'success': True,
                'downloads': downloads_list
            }
        elif list_type == 's3':
            # List S3 objects
            bucket = command_dict.get('bucket')
            prefix = command_dict.get('key', '')
            
            if not bucket:
                return {'success': False, 'error': 'Bucket is required for listing S3 objects'}
            
            # Build s5cmd command for listing
            s5cmd_args = ["ls", f"s3://{bucket}/{prefix}"]
            
            # Execute the list command
            objects = []
            error_messages = []
            
            def collect_output(output_type, line):
                if output_type == "stdout" and line.strip():
                    # Parse s5cmd output and extract object info
                    # Format: <date> <time> <size> <key>
                    parts = line.split()
                    if len(parts) >= 4:
                        date_str = parts[0]
                        time_str = parts[1]
                        size_str = parts[2]
                        key = ' '.join(parts[3:])
                        
                        objects.append({
                            'key': key,
                            'size': size_str,
                            'last_modified': f"{date_str} {time_str}"
                        })
                elif output_type == "stderr":
                    error_messages.append(line)
                    logger.warning(f"s5cmd stderr: {line}")
            
            result = await self.controller.execute_and_wait(
                command=s5cmd_args,
                callback=collect_output
            )
            
            if not result['success'] and error_messages:
                error_detail = '\n'.join(error_messages)
                logger.error(f"List failed with errors: {error_detail}")
            
            return {
                'success': result['success'],
                'objects': objects if result['success'] else [],
                'error': None if result['success'] else f"Failed to list objects: {result['state']}",
                'error_details': '\n'.join(error_messages) if error_messages else None
            }
        else:
            return {'success': False, 'error': f'Unknown list type: {list_type}'}
    
    async def _handle_control_command(self, command_type: str, command_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pause, resume, or cancel commands"""
        download_id = command_dict.get('download_id')
        
        if not download_id:
            return {'success': False, 'error': f'download_id is required for {command_type} command'}
        
        if download_id not in self.active_downloads:
            return {'success': False, 'error': f'Download ID {download_id} not found'}
        
        download_info = self.active_downloads[download_id]
        
        if command_type == 'pause':
            # Pause the download
            success = await self.controller.pause()
            if success:
                download_info['status'] = 'paused'
                download_info['pause_time'] = time.time()
                
                # Reset notification flag if it was set
                if 'notification_sent' in download_info:
                    download_info.pop('notification_sent')
                    
            return {
                'success': success,
                'download_id': download_id,
                'status': download_info['status'],
                'message': f"Download {download_id} {'paused successfully' if success else 'could not be paused'}"
            }
            
        elif command_type == 'resume':
            # Resume the download
            success = await self.controller.resume()
            if success:
                # Update status
                download_info['status'] = 'downloading'
                download_info['resume_time'] = time.time()
                
                # Reset notification flag if it was set
                if 'notification_sent' in download_info:
                    download_info.pop('notification_sent')
                    
            return {
                'success': success,
                'download_id': download_id,
                'status': download_info['status'],
                'message': f"Download {download_id} {'resumed successfully' if success else 'could not be resumed'}"
            }
            
        elif command_type == 'cancel':
            # Cancel the download
            success = await self.controller.cancel()
            if success:
                download_info['status'] = 'cancelled'
                download_info['end_time'] = time.time()
                
                # Mark the download as having sent a notification
                # This will remove it from the status report after the timeout
                download_info['notification_sent'] = False
                
            return {
                'success': success,
                'download_id': download_id,
                'status': download_info['status'],
                'message': f"Download {download_id} {'cancelled successfully' if success else 'could not be cancelled'}"
            }
    
    def get_download_status(self, download_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a specific download
        
        Args:
            download_id: The ID of the download
            
        Returns:
            Status dictionary or None if not found
        """
        if download_id not in self.active_downloads:
            return None
            
        download_info = self.active_downloads[download_id]
        return {
            'download_id': download_id,
            'bucket': download_info.get('bucket'),
            'key': download_info.get('key'),
            'status': download_info.get('status'),
            'progress': download_info.get('progress', 0),
            'destination': download_info.get('destination'),
            'start_time': download_info.get('start_time'),
            'end_time': download_info.get('end_time', None)
        }

    async def monitor_active_downloads(self) -> None:
        """
        Periodically monitor active downloads and log their status
        """
        active_count = 0
        for download_id, info in self.active_downloads.items():
            if info.get('status') in ['downloading', 'paused']:
                active_count += 1
                progress = info.get('progress', 0)
                logger.info(f"Active download: {download_id}, status: {info.get('status')}, progress: {progress}%")
        
        return active_count

    def cleanup_completed_downloads(self, max_age_seconds: int = 3600) -> int:
        """
        Remove completed/failed/cancelled downloads older than the specified age
        
        Args:
            max_age_seconds: Maximum age in seconds to keep completed downloads
                
        Returns:
            Number of downloads removed
        """
        current_time = time.time()
        to_remove = []
        
        for download_id, info in self.active_downloads.items():
            status = info.get('status')
            end_time = info.get('end_time')
            
            if status in ['completed', 'failed', 'cancelled', 'error', 'timeout'] and end_time:
                # For failed downloads, check both age and if notification has been sent
                if status in ['failed', 'cancelled', 'error', 'timeout']:
                    # If notification has been sent and at least 60 seconds have passed
                    # since the download ended, we can remove it
                    notification_sent = info.get('notification_sent', False)
                    notification_age = current_time - end_time
                    
                    if notification_sent and notification_age > 60:
                        to_remove.append(download_id)
                # For completed downloads, use the regular max_age_seconds
                elif status == 'completed' and current_time - end_time > max_age_seconds:
                    to_remove.append(download_id)
        
        # Remove the old downloads
        for download_id in to_remove:
            self.active_downloads.pop(download_id, None)
            logger.info(f"Removed download {download_id} from tracking")
            
        return len(to_remove)

    def check_disk_space(self, path: str = "./") -> Dict[str, float]:
        """
        Check available disk space
        
        Args:
            path: Path to check
            
        Returns:
            Dictionary with total, used, and free space in GB
        """
        import shutil
        total, used, free = shutil.disk_usage(path)
        # Convert to GB
        total_gb = total / (1024 * 1024 * 1024)
        used_gb = used / (1024 * 1024 * 1024)
        free_gb = free / (1024 * 1024 * 1024)
        
        return {
            'total_gb': total_gb,
            'used_gb': used_gb,
            'free_gb': free_gb
        }