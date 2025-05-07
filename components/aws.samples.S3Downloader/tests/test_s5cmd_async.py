import asyncio
import argparse
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.s5cmd_async import AsyncS5CommandController

async def example_usage(bucket: str, key: str = "", dry_run: bool = True):
    """
    Example of basic s5cmd usage with AsyncS5CommandController
    Args:
        bucket: S3 bucket name
        key: Optional key prefix to list
        dry_run: Whether to use the --dry-run flag
    """
    controller = AsyncS5CommandController()

    # Define a callback function to handle real-time output
    def output_callback(output_type, line):
        prefix = "[STDERR]" if output_type == "stderr" else "[STDOUT]"
        print(f"{prefix} {line}")

    command = []
    global_options = []
    if dry_run:
        global_options.append("--dry-run")
    
    # Build the command
    bucket_path = f"s3://{bucket}/"
    if key:
        bucket_path += key
    command = command + ["ls", bucket_path]

    print(f"Executing: s5cmd {' '.join(command)}")

    # Execute a command and wait for completion
    result = await controller.execute_blocking(
        command,
        global_options,
        callback=output_callback
    )

    # Print the result
    print(f"\nCommand completed with status: {result['state']}")
    print(f"Success: {result['success']}")
    print(f"Return code: {result['return_code']}")

    return result

async def control_example(bucket: str, key: str, destination: str, dry_run: bool = True):
    """
    Example demonstrating control features (pause, resume, cancel)
    Args:
        bucket: S3 bucket name
        key: S3 object key or prefix to copy
        destination: Local destination directory
        dry_run: Whether to use the --dry-run flag
    """
    controller = AsyncS5CommandController()

    # Define a callback function
    def output_callback(output_type, line):
        prefix = "[ERROR]" if output_type == "stderr" else "[INFO]"
        print(f"{prefix} {line}")

    # Build the command
    command = []
    global_options = []
    if dry_run:
        global_options.append("--dry-run")

    s3_path = f"s3://{bucket}/{key}"
    command = command + ["cp", s3_path, destination]

    # Start a command
    print(f"Starting command: s5cmd {' '.join(command)}")
    await controller.execute(
        command,
        global_options,
        callback=output_callback
    )

    # Let it run for a bit
    print("Command is running, waiting 2 seconds...")
    await asyncio.sleep(2)

    # Pause the command
    print("Pausing command...")
    paused = await controller.pause()
    print(f"Pause {'successful' if paused else 'failed'}")
    print(f"Current state: {controller.get_state()}")

    # Wait while paused
    print("Command is paused, waiting 2 seconds...")
    await asyncio.sleep(2)

    # Resume the command
    print("Resuming command...")
    resumed = await controller.resume()
    print(f"Resume {'successful' if resumed else 'failed'}")
    print(f"Current state: {controller.get_state()}")

    # Let it run a bit more
    print("Command is running again, waiting 2 seconds...")
    await asyncio.sleep(2)

    # Cancel the command
    print("Cancelling command...")
    cancelled = await controller.cancel()
    print(f"Cancel {'successful' if cancelled else 'failed'}")

    # Wait for it to finish
    result = await controller.wait()

    # Print the result
    print(f"\nCommand finished with status: {result['state']}")
    print(f"Success: {result['success']}")
    print(f"Return code: {result['return_code']}")

    return result

async def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(description='S5CMD Async Controller Examples')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--key', default='', help='S3 object key or prefix')
    parser.add_argument('--destination', default='./downloads/', help='Local destination directory')
    parser.add_argument('--no-dry-run', action='store_true', help='Execute commands for real (without --dry-run)')
    parser.add_argument('--example', choices=['basic', 'control', 'both'], default='both',
    help='Which example to run: basic, control, or both')
    args = parser.parse_args()
    dry_run = not args.no_dry_run
    # If key ends with '/', treat as directory and add wildcard
    if args.key.endswith('/'):
        new_key = f"{args.key}*"

    print("AsyncS5CommandController Examples")
    print("===============================")

    try:
        if args.example in ['basic', 'both']:
            print("\n=== Basic Usage Example ===")
            await example_usage(args.bucket, new_key, dry_run)
        
        if args.example in ['control', 'both']:
            print("\n=== Control Example ===")
            await control_example(args.bucket, new_key, args.destination, dry_run)
            
    except KeyboardInterrupt:
        print("\nExamples interrupted by user")
    except Exception as e:
        print(f"\nError running examples: {e}")

if __name__ == "__main__":
    asyncio.run(main())