import asyncio
import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.logging_config import get_logger
from src.mqtt_interface import MQTTInterface
from src.mock_mqtt import MockMQTTClient
from src.s3_command_service import S3CommandService

logger = get_logger(__name__)

# Global variables for clean shutdown
service = None
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Handle Ctrl+C signals"""
    logger.info("Shutdown signal received")
    shutdown_event.set()

def print_help():
    """Print help information"""
    print("\nAvailable Commands:")
    print("  help                             - Show this help message")
    print("  status                           - Show system status (active downloads and disk space)")
    print("  download <bucket> <key> [dest]   - Download an S3 object")
    print("  download-model <bucket> <key> [dest] --model-meta <json> - Download a model with metadata")
    print("    Example: download-model my-bucket model.bin ./models --model-meta {\"model_id\":\"Qwen2.5-VL-7B\"}")
    print("  list downloads                   - List active downloads")
    print("  list s3 <bucket> [prefix]        - List S3 objects")
    print("  list models                      - List all tracked models")
    print("  get-model <model-id>             - Get details about a specific model")
    print("  add-model <model-id> <local-path> - Add a model directly (without downloading)")
    print("  delete-model <model-id>          - Delete a model from tracking")
    print("  pause <download-id>               - Pause a download")
    print("  resume <download-id>              - Resume a paused download")
    print("  cancel <download-id>              - Cancel a download")
    print("  details <download-id>             - Get detailed information about a download")
    print("  disk-space                       - Check available disk space")
    print("  exit/quit                        - Exit the program")
    print()

async def run_cli_interface(mqtt_client: MockMQTTClient, command_topic: str):
    """
    Run a CLI interface for interacting with the service using non-blocking input
    
    Args:
        mqtt_client: The mock MQTT client
        command_topic: The topic to publish commands to
    """
    print("\nS3 Command Service CLI")
    print("====================")
    print("Type 'help' for available commands")
    
    input_task = None
    
    while not shutdown_event.is_set():
        try:
            # Create input task if it doesn't exist
            if input_task is None:
                input_task = asyncio.create_task(asyncio.to_thread(input, "command> "))
            
            # Wait for input with a timeout to allow checking shutdown event
            done, pending = await asyncio.wait(
                [input_task], 
                timeout=0.1,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Check if we need to exit
            if shutdown_event.is_set():
                if pending:
                    for task in pending:
                        task.cancel()
                break
            
            # Process input if ready
            if done:
                input_task = None  # Reset for next input
                user_input = done.pop().result().strip()
                
                if not user_input:
                    continue
                
                # Handle exit command
                if user_input.lower() in ['exit', 'quit']:
                    print("Exiting...")
                    shutdown_event.set()
                    break
                
                # Handle other commands
                elif user_input.lower() == 'help':
                    print_help()

                elif user_input.lower().startswith('download-model'):
                    # Parse download-model command
                    parts = user_input.split()
                    
                    # Basic validation - require at minimum bucket and key
                    if len(parts) < 3:
                        print("Usage: download-model <bucket> <key> [destination] --model-meta <json_metadata>")
                        print("Example: download-model my-bucket model.bin ./models --model-meta {\"model_id\":\"Qwen2.5-VL-7B-Instruct-AWQ\"}")
                        continue
                    
                    # Extract basic parameters
                    bucket = parts[1]
                    key = parts[2]
                    
                    # Find the metadata flag position
                    try:
                        meta_index = parts.index("--model-meta")
                    except ValueError:
                        print("Error: --model-meta flag is required")
                        print("Example: --model-meta {\"model_id\":\"Qwen2.5-VL-7B-Instruct-AWQ\"}")
                        continue
                    
                    # Determine the destination
                    if meta_index > 3:  # If there's at least one part between key and --model-meta
                        destination = parts[3]
                    else:
                        destination = "./downloads"  # Default destination
                    
                    # Ensure metadata is provided
                    if meta_index == len(parts) - 1:
                        print("Error: --model-meta flag requires a JSON metadata value")
                        print("Example: --model-meta {\"model_id\":\"Qwen2.5-VL-7B-Instruct-AWQ\"}")
                        continue
                    
                    # Parse model metadata as JSON
                    try:
                        # The metadata could be spread across multiple parts if it contains spaces
                        # Join all parts after the --model-meta flag
                        meta_json = ' '.join(parts[meta_index + 1:])
                        model_meta = json.loads(meta_json)
                        
                        # Ensure model_id is present
                        if 'model_id' not in model_meta:
                            filename = os.path.basename(key)
                            model_id = os.path.splitext(filename)[0]  # Use filename as model_id
                            model_meta['model_id'] = model_id
                            print(f"No model_id specified, using filename: {model_id}")
                        
                        # Add timestamp
                        model_meta['last_updated'] = datetime.now().timestamp()
                        
                        # Print debug information
                        print(f"Debug information:")
                        print(f"  Bucket: {bucket}")
                        print(f"  Key: {key}")
                        print(f"  Destination: {destination}")
                        print(f"  Model metadata: {json.dumps(model_meta, indent=2)}")
                        
                        # Create the command
                        command = {
                            'command': 'download',
                            'bucket': bucket,
                            'key': key,
                            'destination': destination,
                            'model_meta': model_meta,
                            'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        }
                        
                        # Inject the command
                        await mqtt_client.inject_message(command_topic, command)
                        print(f"Download model command sent: {json.dumps(command, indent=2)}")
                        
                    except json.JSONDecodeError as e:
                        print(f"Error: Invalid JSON metadata: {e}")
                        print("Model metadata must be valid JSON")
                        print("Example: --model-meta {\"model_id\":\"Qwen2.5-VL-7B-Instruct-AWQ\"}")
                        continue

                elif user_input.lower().startswith('download'):
                    # Parse download command
                    parts = user_input.split()
                    if len(parts) < 3:
                        print("Usage: download <bucket> <key> [destination]")
                        continue
                    
                    bucket = parts[1]
                    key = parts[2]
                    destination = parts[3] if len(parts) > 3 else "./downloads"
                    
                    command = {
                        'command': 'download',
                        'bucket': bucket,
                        'key': key,
                        'destination': destination,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"Download command sent: {json.dumps(command, indent=2)}")
                
                elif user_input.lower().startswith('list models'):
                    # Create list models command
                    command = {
                        'command': 'model_list',
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"List models command sent")
                    
                    # Wait for the response
                    try:
                        topic, payload = await asyncio.wait_for(mqtt_client.get_next_response(), timeout=5.0)
                        
                        # Check if the response was successful
                        if payload.get('success', False) and 'models' in payload:
                            models = payload['models']
                            if models:
                                print("\nModels:")
                                for model_id, model_data in models.items():
                                    print(f"  - {model_id}")
                                    print(f"    Local path: {model_data.get('local_path', 'Unknown')}")
                                    if 'last_updated' in model_data:
                                        timestamp = model_data['last_updated']
                                        date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                                        print(f"    Last updated: {date_str}")
                                    # Print other optional fields if present
                                    for field in ['model_name', 'model_version', 'modality']:
                                        if field in model_data:
                                            print(f"    {field.replace('_', ' ').title()}: {model_data[field]}")
                            else:
                                print("No models found")
                        else:
                            # If the command failed, show the error
                            print(f"List models command failed: {payload.get('error', 'Unknown error')}")
                                
                    except asyncio.TimeoutError:
                        print("List models request timed out")
                
                elif user_input.lower().startswith('list'):
                    # Parse list command
                    parts = user_input.split()
                    list_type = parts[1] if len(parts) > 1 else 'downloads'
                    
                    command = {
                        'command': 'list',
                        'listType': list_type,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    if list_type == 's3' and len(parts) > 2:
                        command['bucket'] = parts[2]
                        if len(parts) > 3:
                            command['key'] = parts[3]
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"List command sent: {json.dumps(command, indent=2)}")
                
                elif user_input.lower().startswith('get-model'):
                    # Parse get-model command
                    parts = user_input.split()
                    if len(parts) < 2:
                        print("Usage: get-model <model-id>")
                        continue
                    
                    model_id = parts[1]
                    command = {
                        'command': 'model_get',
                        'model_id': model_id,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"Get model command sent for {model_id}")
                    
                    # Wait for the response
                    try:
                        topic, payload = await asyncio.wait_for(mqtt_client.get_next_response(), timeout=5.0)
                        
                        # Check if the response was successful
                        if payload.get('success', False) and 'model' in payload:
                            model = payload['model']
                            print(f"\nModel: {model.get('model_id', 'Unknown')}")
                            print(f"Local path: {model.get('local_path', 'Unknown')}")
                            
                            # Print optional fields if present
                            for field, value in model.items():
                                if field not in ['model_id', 'local_path']:
                                    if field == 'last_updated':
                                        date_str = datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
                                        print(f"{field.replace('_', ' ').title()}: {date_str}")
                                    else:
                                        print(f"{field.replace('_', ' ').title()}: {value}")
                        else:
                            # If the command failed, show the error
                            print(f"Get model command failed: {payload.get('error', 'Unknown error')}")
                                
                    except asyncio.TimeoutError:
                        print("Get model request timed out")
                
                elif user_input.lower().startswith('add-model'):
                    # Parse add-model command
                    parts = user_input.split()
                    if len(parts) < 3:
                        print("Usage: add-model <model-id> <local-path> [model-name] [model-version]")
                        continue
                    
                    model_id = parts[1]
                    local_path = parts[2]
                    
                    # Create model metadata
                    model_meta = {
                        "model_id": model_id,
                        "local_path": local_path,
                        "last_updated": datetime.now().timestamp()
                    }
                    
                    # Add optional fields if provided
                    if len(parts) > 3:
                        model_meta["model_name"] = parts[3]
                    if len(parts) > 4:
                        model_meta["model_version"] = parts[4]
                    
                    command = {
                        'command': 'model_add',
                        'model_meta': model_meta,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"Add model command sent: {json.dumps(command, indent=2)}")
                
                elif user_input.lower().startswith('delete-model'):
                    # Parse delete-model command
                    parts = user_input.split()
                    if len(parts) < 2:
                        print("Usage: delete-model <model-id>")
                        continue
                    
                    model_id = parts[1]
                    command = {
                        'command': 'model_delete',
                        'model_id': model_id,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"Delete model command sent for {model_id}")
                
                elif user_input.lower().startswith(('pause', 'resume', 'cancel')):
                    # Parse control command
                    parts = user_input.split()
                    if len(parts) < 2:
                        print(f"Usage: {parts[0]} <download-id>")
                        continue
                    
                    command = {
                        'command': parts[0],
                        'download_id': parts[1],
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"{parts[0].capitalize()} command sent for download {parts[1]}")

                elif user_input.lower().startswith('details'):
                    # Parse details command
                    parts = user_input.split()
                    if len(parts) < 2:
                        print("Usage: details <download-id>")
                        continue
                        
                    download_id = parts[1]
                    
                    # Get download details
                    command = {
                        'command': 'getDetails',
                        'download_id': download_id,
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print(f"Details request sent for download {download_id}")

                elif user_input.lower() == 'status':
                    # Create and send a status command
                    command = {
                        'command': 'status',
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print("Status command sent, waiting for response...")
                    
                    # Wait for the response to our command
                    try:
                        topic, payload = await asyncio.wait_for(mqtt_client.get_next_response(), timeout=5.0)
                        
                        # Check if the response was successful
                        if payload.get('success', False):
                            if 'system_info' in payload:
                                system_info = payload['system_info']
                                print("\nSystem Status:")
                                print(f"Active Downloads: {system_info.get('active_downloads', 0)}")
                                
                                if system_info.get('downloads'):
                                    print("\nDownloads:")
                                    for download in system_info['downloads']:
                                        print(f"  - {download['download_id']} ({download['status']}): {download.get('progress', 0):.1f}%")
                                        print(f"    Bucket: {download.get('bucket')}, Key: {download.get('key')}")
                                        print(f"    Destination: {download.get('destination')}")
                                else:
                                    print("No active downloads")
                                    
                                if 'disk_space' in system_info:
                                    disk = system_info['disk_space']
                                    print(f"\nDisk Space:")
                                    print(f"  - Total: {disk.get('total_gb', 0):.2f} GB")
                                    print(f"  - Used: {disk.get('used_gb', 0):.2f} GB ({disk.get('percent_used', 0):.1f}%)")
                                    print(f"  - Free: {disk.get('free_gb', 0):.2f} GB")
                            else:
                                # Fall back to printing the raw response if structure doesn't match
                                print(f"Status response: {json.dumps(payload, indent=2)}")
                        else:
                            # If the command failed, show the error
                            print(f"Status command failed: {payload.get('error', 'Unknown error')}")
                            if 'details' in payload:
                                print(f"Details: {payload['details']}")
                                
                    except asyncio.TimeoutError:
                        print("Status request timed out")
                        
                elif user_input.lower() == 'disk-space':
                    # Check disk space
                    command = {
                        'command': 'disk-space',
                        'command_id': f"cli-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    }
                    
                    # Inject the command
                    await mqtt_client.inject_message(command_topic, command)
                    print("Checking disk space...")

                else:
                    print(f"Unknown command: {user_input}")
                    print("Type 'help' for available commands")
                    
        except asyncio.CancelledError:
            logger.info("CLI interface task cancelled")
            break
        except Exception as e:
            print(f"Error in CLI interface: {e}")
            logger.exception("CLI interface error")
            # Reset input task on error
            input_task = None
    
    # Clean up any pending input task
    if input_task and not input_task.done():
        input_task.cancel()
        try:
            await input_task
        except (asyncio.CancelledError, Exception):
            pass
            
    logger.info("CLI interface stopped")

async def log_responses(mqtt_client: MockMQTTClient):
    """
    Log all responses from the service
    
    Args:
        mqtt_client: The mock MQTT client
    """
    while not shutdown_event.is_set():
        try:
            # Wait for a response with a timeout
            topic, payload = await asyncio.wait_for(
                mqtt_client.get_next_response(),
                timeout=0.5
            )
            
            # Skip CLI messages
            if topic == "CLI":
                continue
                
            logger.info(f"Response on {topic}: {json.dumps(payload)}")
            
        except asyncio.TimeoutError:
            # Just a timeout, continue
            pass
        except Exception as e:
            logger.error(f"Error in response logger: {e}")
            
        # Check if we should exit
        if shutdown_event.is_set():
            break

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='S3 Command Service Test')
    parser.add_argument('--device-id', default='test-device-001', help='Device ID')
    parser.add_argument('--topic-prefix', default='device/s3', help='MQTT topic prefix')
    parser.add_argument('--download-dir', default='./downloads', help='Default download directory')
    parser.add_argument('--process-interval', type=float, default=10.0, help='Status update interval in seconds')
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create download directory if it doesn't exist
    os.makedirs(args.download_dir, exist_ok=True)
    
    # Create mock MQTT client
    mqtt_client = MockMQTTClient()
    
    # Create and start service
    global service
    service = S3CommandService(
        mqtt_client=mqtt_client,
        device_id=args.device_id,
        topic_prefix=args.topic_prefix,
        default_download_dir=args.download_dir,
        process_interval=args.process_interval
    )
    
    # Start the service
    started = await service.start()
    if not started:
        logger.error("Failed to start service")
        return 1
        
    logger.info(f"Service started for device {args.device_id}")
    logger.info(f"Command topic: {service.command_topic}")
    logger.info(f"Response topic: {service.response_topic}")
    logger.info(f"Status topic: {service.status_topic}")
    
    # Start response logger
    response_logger_task = asyncio.create_task(log_responses(mqtt_client))
    
    # Start CLI interface
    cli_task = asyncio.create_task(run_cli_interface(mqtt_client, service.command_topic))
    
    # Wait for shutdown signal
    await shutdown_event.wait()
    
    # Clean up
    if service:
        await service.stop()
        
    # Cancel tasks
    cli_task.cancel()
    response_logger_task.cancel()
    
    try:
        await cli_task
    except asyncio.CancelledError:
        pass
    
    try:
        await response_logger_task
    except asyncio.CancelledError:
        pass
    
    logger.info("Service stopped")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nProgram interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)