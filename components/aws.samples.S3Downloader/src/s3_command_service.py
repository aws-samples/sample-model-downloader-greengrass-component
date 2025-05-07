import asyncio
import json
import time
import uuid
import os
from typing import Dict, Any, Optional

from .utils.logging_config import get_logger
from .s3_command_manager import S3CommandManager
from .model_shadow_manager import ModelShadowManager
from .mqtt_interface import MQTTInterface

logger = get_logger(__name__)


class S3CommandService:
    """
    Main service that bridges MQTT communication with S3CommandManager
    """
    
    def __init__(self, 
                 mqtt_client: MQTTInterface, 
                 device_id: str,
                 topic_prefix: str = "device/s3",
                 default_download_dir: str = "./downloads",
                 process_interval: float = 5.0,
                 idle_process_interval: float = 60.0):
        """
        Initialize the S3CommandService
        
        Args:
            mqtt_client: MQTT client implementing MQTTInterface
            device_id: Device identifier
            topic_prefix: MQTT topic prefix
            default_download_dir: Default directory for downloads
            process_interval: Interval in seconds for periodic processing
            idle_process_interval: Interval in seconds for periodic processing when no active download
        """
        self.mqtt_client = mqtt_client
        self.device_id = device_id
        self.topic_prefix = topic_prefix
        self.process_interval = process_interval
        self.idle_process_interval = idle_process_interval
        
        # Create command manager
        self.command_manager = S3CommandManager(device_id, default_download_dir)

        # Create model shadow manager
        self.model_shadow_manager = ModelShadowManager(mqtt_client, device_id)

        # Setup topics
        self.command_topic = f"{topic_prefix}/{device_id}/commands"
        self.response_topic = f"{topic_prefix}/{device_id}/responses"
        self.status_topic = f"{topic_prefix}/{device_id}/status"
        
        # Control flags
        self.running = False
        self._process_task = None
        
    async def start(self) -> bool:
        """
        Start the service
        
        Returns:
            Success status
        """
        logger.info(f"Starting S3CommandService for device {self.device_id}")
        
        # Connect MQTT client
        connected = await self.mqtt_client.connect()
        if not connected:
            logger.error("Failed to connect MQTT client")
            return False
            
        # Subscribe to command topic
        subscribed = await self.mqtt_client.subscribe(
            self.command_topic, 
            self._handle_command_message
        )
        if not subscribed:
            logger.error(f"Failed to subscribe to {self.command_topic}")
            await self.mqtt_client.disconnect()
            return False

        # Initialize model shadow manager
        shadow_init = await self.model_shadow_manager.initialize()
        if not shadow_init:
            logger.warning("Failed to initialize model shadow manager, continuing without model tracking")
        else:
            logger.info("Model shadow manager initialized successfully")
            
        # Start periodic processing
        self.running = True
        self._process_task = asyncio.create_task(self._periodic_processing())
        
        # Publish initial status
        await self._publish_status()
        
        return True
        
    async def stop(self) -> None:
        """Stop the service"""
        logger.info("Stopping S3CommandService")
        self.running = False
        
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
            
        # Unsubscribe and disconnect
        await self.mqtt_client.unsubscribe(self.command_topic)
        await self.mqtt_client.disconnect()
        
    async def _handle_command_message(self, topic: str, payload: Dict[str, Any]) -> None:
        """
        Handle incoming command messages
        
        Args:
            topic: MQTT topic
            payload: Command payload
        """
        # Auto-assign a command ID if not provided
        if 'command_id' not in payload:
            payload['command_id'] = f"auto-{str(uuid.uuid4())[:8]}"
            logger.info(f"Auto-assigned command ID: {payload['command_id']}")

        command_id = payload.get('command_id', 'unknown')
        
        try:
            logger.info(f"Received command: {json.dumps(payload)}")
            
            # Validate basic structure
            if not isinstance(payload, dict):
                error_msg = "Invalid command format: payload must be a JSON object"
                logger.error(error_msg)
                
                # Send concise error in response topic
                error_response = {
                    'success': False,
                    'error': error_msg,
                    'errorCode': 'INVALID_FORMAT',
                    'command_id': command_id
                }
                await self.mqtt_client.publish(self.response_topic, error_response)
                
                # Send detailed error in status topic
                error_status = {
                    'device_id': self.device_id,
                    'timestamp': time.time(),
                    'command_error': {
                        'command_id': command_id,
                        'message': error_msg,
                        'details': f"Expected JSON object, received: {type(payload).__name__}",
                        'payload': str(payload)[:200]  # Truncate long payloads
                    }
                }
                await self.mqtt_client.publish(self.status_topic, error_status)
                return
                    
            command_type = payload.get('command')
            if not command_type:
                error_msg = "Invalid command format: missing 'command' field"
                logger.error(error_msg)
                
                # Send concise error in response topic
                error_response = {
                    'success': False,
                    'error': error_msg,
                    'errorCode': 'MISSING_COMMAND',
                    'command_id': command_id
                }
                await self.mqtt_client.publish(self.response_topic, error_response)
                
                # Send detailed error in status topic
                error_status = {
                    'device_id': self.device_id,
                    'timestamp': time.time(),
                    'command_error': {
                        'command_id': command_id,
                        'message': error_msg,
                        'details': f"Command payload is missing required 'command' field",
                        'received_fields': list(payload.keys())
                    }
                }
                await self.mqtt_client.publish(self.status_topic, error_status)
                return
                
            # Process the command
            try:
                command_id = payload.get('command_id')

                # Handle model-related commands directly
                if command_type == 'model_add':
                    # Add or update a model directly (not through download)
                    response = await self._handle_model_add(payload)
                elif command_type == 'model_get':
                    # Get details of a specific model
                    response = await self._handle_model_get(payload)
                elif command_type == 'model_list':
                    # List all models
                    response = await self._handle_model_list()
                elif command_type == 'model_delete':
                    # Delete a model
                    response = await self._handle_model_delete(payload)
                else:
                    # Handle regular commands, with special handling for model downloads
                    if command_type == 'download' and 'model_meta' in payload:
                        # Store download command with model metadata
                        logger.info(f"Download command includes model metadata: {json.dumps(payload.get('model_meta'))}")
                        response = await self.command_manager.execute_command(payload)
                    else:
                        # Regular command execution
                        response = await self.command_manager.execute_command(payload)                
                
                if command_id:
                    response['command_id'] = command_id
                    
                # Publish the response
                await self.mqtt_client.publish(
                    self.response_topic,
                    response
                )
                
                # If there was an error in command execution, send additional details in status
                if not response.get('success', False) and 'error' in response:
                    # Get error details and truncate if too long
                    error_details = response.get('error_details', '')
                    if isinstance(error_details, str) and len(error_details) > 256:
                        # Keep only the last 256 characters of error details
                        truncated_details = "..." + error_details[-256:]
                    else:
                        truncated_details = error_details

                    error_status = {
                        'device_id': self.device_id,
                        'timestamp': time.time(),
                        'command_error': {
                            'command_id': command_id,
                            'command': command_type,
                            'message': response.get('error'),
                            'details': response.get('error_details', ''),
                            # Include any additional context
                            'system_state': {
                                'disk_space': self.command_manager.check_disk_space() if hasattr(self.command_manager, 'check_disk_space') else None,
                                'active_downloads': len(self.command_manager.active_downloads)
                            }
                        }
                    }
                    await self.mqtt_client.publish(self.status_topic, error_status)
                
            except Exception as e:
                logger.error(f"Error executing command: {e}")
                
                # Concise error response
                error_response = {
                    'success': False,
                    'error': f"Failed to execute {command_type} command: {str(e)}",
                    'errorCode': 'EXECUTION_ERROR',
                    'command_id': command_id
                }
                await self.mqtt_client.publish(self.response_topic, error_response)
                
                # Detailed error status
                import traceback
                error_status = {
                    'device_id': self.device_id,
                    'timestamp': time.time(),
                    'command_error': {
                        'command_id': command_id,
                        'command': command_type,
                        'message': str(e),
                        'stack_trace': traceback.format_exc()[-256:],
                        'system_state': {
                            'disk_space': self.command_manager.check_disk_space() if hasattr(self.command_manager, 'check_disk_space') else None,
                            'active_downloads': len(self.command_manager.active_downloads)
                        }
                    }
                }
                await self.mqtt_client.publish(self.status_topic, error_status)
                
        except Exception as e:
            logger.error(f"Unhandled error processing command: {e}")
            
            import traceback
            # Send error response - basic info
            error_response = {
                'success': False,
                'error': f"Unhandled error: {str(e)}",
                'errorCode': 'UNHANDLED_ERROR',
                'command_id': command_id
            }
            
            await self.mqtt_client.publish(self.response_topic, error_response)
            
            # Send detailed error status
            error_status = {
                'device_id': self.device_id,
                'timestamp': time.time(),
                'command_error': {
                    'command_id': command_id,
                    'message': str(e),
                    'stack_trace': traceback.format_exc()[-256:],
                    'raw_payload': str(payload)[:200]  # Truncate for safety
                }
            }
            await self.mqtt_client.publish(self.status_topic, error_status)

    async def _handle_model_add(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle model_add command
        
        Args:
            payload: Command payload with model metadata in model_meta field
        """
        model_meta = payload.get('model_meta')
        if not model_meta:
            return {'success': False, 'error': 'Missing model_meta in model_add command'}
            
        # Validate required fields
        if 'model_id' not in model_meta or 'local_path' not in model_meta:
            return {'success': False, 'error': 'model_meta must contain model_id and local_path'}
            
        return await self.model_shadow_manager.add_or_update_model(model_meta)
        
    async def _handle_model_get(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle model_get command
        
        Args:
            payload: Command payload with model_id
        """
        model_id = payload.get('model_id')
        if not model_id:
            return {'success': False, 'error': 'Missing model_id in model_get command'}
            
        return await self.model_shadow_manager.get_model(model_id)
        
    async def _handle_model_list(self) -> Dict[str, Any]:
        """
        Handle model_list command
        """
        return await self.model_shadow_manager.get_all_models()
        
    async def _handle_model_delete(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle model_delete command
        
        Args:
            payload: Command payload with model_id
        """
        model_id = payload.get('model_id')
        if not model_id:
            return {'success': False, 'error': 'Missing model_id in model_delete command'}
            
        return await self.model_shadow_manager.delete_model(model_id)
    
    async def _periodic_processing(self) -> None:
        """Periodic tasks including status updates and notification processing"""
        try:
            while self.running:
                try:
                    # Process download notifications
                    await self._process_download_notifications()
                    
                    # Monitor active downloads
                    active_count = await self.command_manager.monitor_active_downloads()
                    
                    # Check for stalled downloads (no progress for a long time)
                    current_time = time.time()
                    for download_id, info in list(self.command_manager.active_downloads.items()):
                        # Only check active downloads
                        if info.get('status') not in ['downloading', 'paused']:
                            continue
                            
                        # Check if download has been updated recently
                        last_update = info.get('last_progress_update')
                        if last_update and (current_time - last_update) > 300:  # 5 minutes
                            logger.warning(f"Download {download_id} may be stalled - no progress for 5 minutes")
                            
                            # Update status to indicate potential stall
                            info['status_note'] = "Download may be stalled - no progress for 5 minutes"
                    
                    # Clean up completed downloads
                    cleaned = self.command_manager.cleanup_completed_downloads()
                    if cleaned > 0:
                        logger.info(f"Cleaned up {cleaned} completed downloads")
                    
                    # Publish status update
                    await self._publish_status()
                    
                    # Check system resources
                    if hasattr(self.command_manager, 'check_disk_space'):
                        disk_info = self.command_manager.check_disk_space()
                        if disk_info and disk_info.get('free_gb', 0) < 1.0:  # Less than 1GB free
                            logger.warning(f"Low disk space: {disk_info.get('free_gb'):.2f} GB free")
                            
                            # Publish low disk space warning
                            await self.mqtt_client.publish(
                                self.status_topic,
                                {
                                    'device_id': self.device_id,
                                    'timestamp': time.time(),
                                    'warning': "Low disk space",
                                    'details': {
                                        'free_gb': disk_info.get('free_gb'),
                                        'total_gb': disk_info.get('total_gb'),
                                        'used_gb': disk_info.get('used_gb')
                                    }
                                }
                            )
                    
                    # Adjust sleep interval based on active downloads
                    sleep_time = self.process_interval if active_count > 0 else self.idle_process_interval
                    
                    # Wait for next interval
                    await asyncio.sleep(sleep_time)
                        
                except Exception as e:
                    logger.error(f"Error in periodic task: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    # Use default interval on error
                    await asyncio.sleep(self.process_interval)
                    
        except asyncio.CancelledError:
            logger.info("Periodic processing task cancelled")
        except Exception as e:
            logger.error(f"Error in periodic processing: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            # Try to report the error
            try:
                await self.mqtt_client.publish(
                    self.status_topic,
                    {
                        'device_id': self.device_id,
                        'timestamp': time.time(),
                        'error': "Periodic processing error",
                        'details': str(e),
                        'stack_trace': traceback.format_exc()[-256:]  # Truncate to last 256 characters
                    }
                )
            except Exception as report_error:
                logger.error(f"Failed to report error via MQTT: {report_error}")

    async def _publish_status(self) -> None:
        """Publish current status to status topic with improved filtering of downloads"""
        try:
            # Get active downloads with better filtering
            active_downloads = []
            
            current_time = time.time()
            notification_timeout = 60  # Show failed downloads for 60 seconds after failure
            
            for download_id, info in self.command_manager.active_downloads.items():
                status = info.get('status')
                
                # Only include downloads based on specific criteria
                if status == 'downloading':
                    # Always include active downloads
                    active_downloads.append({
                        'download_id': download_id,
                        'bucket': info.get('bucket'),
                        'key': info.get('key'),
                        'status': status,
                        'progress': float(info.get('progress', 0.0)),  # Ensure it's a float
                        'destination': info.get('destination')
                    })
                elif status == 'paused':
                    # Always include paused downloads
                    active_downloads.append({
                        'download_id': download_id,
                        'bucket': info.get('bucket'),
                        'key': info.get('key'),
                        'status': status,
                        'progress': float(info.get('progress', 0.0)),
                        'destination': info.get('destination')
                    })
                elif status == 'failed' or status == 'cancelled' or status == 'error' or status == 'timeout':
                    # Only include failed/cancelled/error downloads if they're recent
                    end_time = info.get('end_time', 0)
                    
                    # If the download failed/cancelled/errored within the notification timeout,
                    # include it in status but only once (until notification is processed)
                    if current_time - end_time < notification_timeout and not info.get('notification_sent', False):
                        active_downloads.append({
                            'download_id': download_id,
                            'bucket': info.get('bucket'),
                            'key': info.get('key'),
                            'status': status,
                            'progress': float(info.get('progress', 0.0)),
                            'destination': info.get('destination')
                        })
                        
                        # Mark that we've included this download in the status
                        # This prevents it from appearing in future status reports
                        info['notification_sent'] = True
            
            # Build status message
            status = {
                'device_id': self.device_id,
                'timestamp': time.time(),
                'active_downloads': len([d for d in active_downloads if d['status'] in ['downloading', 'paused']]),
                'downloads': active_downloads
            }
            
            # Publish status
            await self.mqtt_client.publish(
                self.status_topic,
                status
            )
            
        except Exception as e:
            logger.error(f"Error publishing status: {e}")

    async def _process_download_notifications(self) -> None:
        """Process and handle download notifications with better status tracking"""
        
        # Check for completed downloads with pending notifications
        for download_id, info in list(self.command_manager.active_downloads.items()):
            if 'completion_notification' in info:
                notification = info.pop('completion_notification')
                
                # Send to response topic
                response_msg = {
                    'event': 'download_completed',
                    'download_id': download_id,
                    'success': notification['success'],
                    'status': notification['status'],
                    'progress': notification['progress'],
                    'command_id': notification.get('command_id')
                }
                
                # Add error information if failed
                if not notification['success']:
                    response_msg['error'] = notification.get('error', 'Download failed')
                
                await self.mqtt_client.publish(
                    self.response_topic,
                    response_msg
                )
                
                # Mark this download as having sent a notification
                # This will prevent it from showing up in future status reports
                info['notification_sent'] = True

                # Handle model metadata if this was a model download and it succeeded
                if notification['success'] and 'model_meta' in info:
                    model_meta = info['model_meta']
                    logger.info(f"Processing model metadata for completed download {download_id}")
                    
                    # Ensure model_id is present
                    if 'model_id' not in model_meta:
                        model_id = os.path.basename(info.get('key', '')).split('.')[0]
                        if not model_id:
                            model_id = f"model-{str(uuid.uuid4())[:8]}"
                        model_meta['model_id'] = model_id
                    
                    # Add file information to model metadata
                    dest_path = info.get('destination', self.command_manager.default_download_dir)
                    file_name = info.get('file_name', os.path.basename(info.get('key', '')))
                    local_path = os.path.join(dest_path, file_name)
                    
                    # Update model metadata with file path
                    model_meta['local_path'] = local_path
                    
                    # Add the model to shadow
                    result = await self.model_shadow_manager.add_or_update_model(model_meta)
                    
                    if result.get('success', False):
                        logger.info(f"Added model metadata to shadow for download {download_id}")
                        
                        # Add model information to response
                        model_response = {
                            'event': 'model_added',
                            'download_id': download_id,
                            'model_id': model_meta.get('model_id'),
                            'local_path': model_meta.get('local_path'),
                            'command_id': notification.get('command_id')
                        }
                        
                        await self.mqtt_client.publish(
                            self.response_topic,
                            model_response
                        )
                    else:
                        logger.error(f"Failed to add model metadata to shadow: {result.get('error')}")
                                
                # Send detailed status if there was an error
                if not notification['success']:
                    error_status = {
                        'device_id': self.device_id,
                        'timestamp': time.time(),
                        'download_error': {
                            'download_id': download_id,
                            'command_id': notification.get('command_id'),
                            'bucket': notification['bucket'],
                            'key': notification['key'],
                            'status': notification['status'],
                            'progress': notification['progress'],
                            'message': notification.get('error', 'Download failed'),
                            'details': notification.get('error_details', ''),
                            'duration': notification['duration']
                        }
                    }
                    await self.mqtt_client.publish(self.status_topic, error_status)

            if 'error_notification' in info:
                notification = info.pop('error_notification')
                command_id = notification.get('command_id') or info.get('command_id')
                
                logger.info(f"Processing error notification with command_id: {command_id}")
                
                # Mark this download as having sent a notification
                info['notification_sent'] = True
                
                # Send to response topic - concise message
                await self.mqtt_client.publish(
                    self.response_topic,
                    {
                        'event': notification.get('event', 'download_error'),
                        'download_id': notification.get('download_id'),
                        'success': False,
                        'status': 'failed',
                        'error': notification.get('error', 'Download failed'),
                        'command_id': command_id
                    }
                )
                
                # Send to status topic - detailed message
                await self.mqtt_client.publish(
                    self.status_topic,
                    {
                        'device_id': self.device_id,
                        'timestamp': time.time(),
                        'download_error': {
                            'download_id': notification.get('download_id'),
                            'command_id': command_id,
                            'bucket': notification.get('bucket'),
                            'key': notification.get('key'),
                            'status': 'failed',
                            'progress': notification.get('progress', 0),
                            'error': notification.get('error', 'Download failed'),
                            'error_details': notification.get('error_details', '')
                        }
                    }
                )