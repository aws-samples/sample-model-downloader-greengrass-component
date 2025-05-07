import asyncio
import argparse
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any

from src.utils.logging_config import get_logger
from src.greengrass_mqtt import GreengrassSDKClient
from src.s3_command_service import S3CommandService

logger = get_logger(__name__)

# Global variables for clean shutdown
service = None
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, shutting down...")
    shutdown_event.set()


class S3CommandComponent:
    """Main Greengrass component for S3 commands"""
    
    def __init__(self, 
                 thing_name: str,
                 download_dir: str,
                 topic_prefix: str = "iot/s3",
                 process_interval: float = 30.0,
                 idle_process_interval: float = 60.0):
        """
        Initialize the S3CommandComponent
        
        Args:
            thing_name: AWS IoT Thing name (device identifier)
            download_dir: Directory for downloading files
            topic_prefix: MQTT topic prefix
            process_interval: Interval for status updates in seconds
        """
        self.thing_name = thing_name
        self.download_dir = download_dir
        self.topic_prefix = topic_prefix
        self.process_interval = process_interval
        self.idle_process_interval = idle_process_interval
        
        # Create the MQTT client
        self.mqtt_client = GreengrassSDKClient()
        
        # Create the S3 command service
        self.command_service = S3CommandService(
            mqtt_client=self.mqtt_client,
            device_id=self.thing_name,
            topic_prefix=self.topic_prefix,
            default_download_dir=download_dir,
            process_interval=self.process_interval,
            idle_process_interval=self.idle_process_interval
        )
        
        # Topic patterns
        self.command_topic = f"{topic_prefix}/{thing_name}/commands"
        self.shadow_topic_prefix = f"$aws/things/{thing_name}/shadow"
        
    async def start(self) -> bool:
        """
        Start the component
        
        Returns:
            Success status
        """
        logger.info(f"Starting S3 command component for thing {self.thing_name}")
        
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Connect to IoT Core
        connected = await self.mqtt_client.connect()
        if not connected:
            logger.error("Failed to connect to IoT Core")
            return False
            
        # Start the command service
        service_started = await self.command_service.start()
        if not service_started:
            logger.error("Failed to start S3 command service")
            await self.mqtt_client.disconnect()
            return False

        logger.info(f"S3 command component started successfully")
        logger.info(f"Command topic: {self.command_topic}")
        logger.info(f"Download directory: {self.download_dir}")
        
        return True
        
    async def stop(self) -> None:
        """Stop the component"""
        logger.info("Stopping S3 command component")
        
        # Stop the command service
        if self.command_service:
            await self.command_service.stop()
            
        # Disconnect from IoT Core
        if self.mqtt_client:
            await self.mqtt_client.disconnect()
            
        logger.info("S3 command component stopped")
        
async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='S3 Command Component')
    parser.add_argument('--thing-name', required=True, help='AWS IoT Thing name')
    parser.add_argument('--download-dir', default='/data/downloads/model', help='Directory for downloads')
    parser.add_argument('--topic-prefix', default='s3downloader', help='MQTT topic prefix')
    parser.add_argument('--process-interval', type=float, default=30.0, help='Status update interval in seconds')
    parser.add_argument('--idle-process-interval', type=float, default=60.0, help='Status update interval when idle in seconds')
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start the component
    global service
    service = S3CommandComponent(
        thing_name=args.thing_name,
        download_dir=args.download_dir,
        topic_prefix=args.topic_prefix,
        process_interval=args.process_interval,
        idle_process_interval=args.idle_process_interval
    )
    
    # Start the component
    started = await service.start()
    if not started:
        logger.error("Failed to start S3 command component")
        return 1
        
    # Keep running until shutdown
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        # Stop the component
        if service:
            await service.stop()
    
    logger.info("S3 command component exited")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Program interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)