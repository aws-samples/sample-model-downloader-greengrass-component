import asyncio
import os
import re
import signal
from typing import List, Dict, Any, Optional, Callable, Coroutine
from enum import Enum
import sys
from .utils.logging_config import get_logger

logger = get_logger(__name__)


class CommandState(Enum):
    """Enum representing the current state of a command execution."""
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"


class AsyncS5CommandController:
    """
    An asynchronous controller class for executing s5cmd commands with real-time
    output capture and control capabilities (pause, resume, cancel).
    """
    def __init__(self, s5cmd_path: str = "s5cmd"):
        """
        Initialize the AsyncS5CommandController.
        
        Args:
            s5cmd_path: Path to the s5cmd executable, defaults to "s5cmd" (assumes it's in PATH)
        """
        self.s5cmd_path = s5cmd_path
        self.process = None
        self.state = CommandState.READY
        self._stopping = False
        self._paused = False
        self._stdout_task = None
        self._stderr_task = None
        self._monitor_task = None
        self._output_callback = None
        
    async def _verify_s5cmd(self):
        """Verify that s5cmd is available and get its version."""
        try:
            process = await asyncio.create_subprocess_exec(
                self.s5cmd_path, "version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.warning(f"s5cmd verification returned non-zero exit code: {process.returncode}")
                logger.warning(f"stderr: {stderr}")
            else:
                logger.info(f"s5cmd version: {stdout.strip()}")
                
            return process.returncode == 0
            
        except (FileNotFoundError, PermissionError) as e:
            logger.warning(f"s5cmd executable not found or not accessible at '{self.s5cmd_path}': {e}")
            raise FileNotFoundError(f"s5cmd executable not found or not accessible at '{self.s5cmd_path}'")

    async def _read_stream(self, stream, is_stderr: bool = False):
        """
        Read output from a stream and process it more effectively for progress updates.
        
        Args:
            stream: The stream to read from
            is_stderr: Whether this stream is stderr (True) or stdout (False)
        """
        output_type = "stderr" if is_stderr else "stdout"
        
        try:
            buffer = b""
            while not stream.at_eof():
                try:
                    # Read a small chunk at a time for more frequent updates
                    chunk = await stream.read(512)  # Smaller chunks
                    if not chunk:  # Empty chunk (EOF)
                        break
                    
                    buffer += chunk
                    
                    # For progress bars, we don't need to process only complete lines
                    # Process what we have immediately
                    if buffer:
                        line_str = buffer.decode('utf-8', errors='replace')
                        
                        # Process entire buffer for progress updates
                        if self._output_callback and is_stderr:
                            await self._call_callback(output_type, line_str)
                        
                        # Try to split on newlines for regular output processing
                        lines = buffer.split(b'\n')
                        # Last element might be incomplete, keep it for the next iteration
                        buffer = lines.pop() if lines else b""
                        
                        # Process complete lines
                        for line in lines:
                            if line:  # Skip empty lines
                                line_str = line.decode('utf-8', errors='replace')
                                # Call the callback if it exists
                                if self._output_callback:
                                    await self._call_callback(output_type, line_str)
                    
                    # Small delay to allow other tasks to run
                    await asyncio.sleep(0.01)
                    
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    # Reset buffer to avoid getting stuck
                    buffer = b""
                    
            # Process any remaining data
            if buffer:
                line_str = buffer.decode('utf-8', errors='replace')
                if self._output_callback:
                    await self._call_callback(output_type, line_str)
                    
        except asyncio.CancelledError:
            logger.debug(f"{output_type} reader task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error reading from {output_type}: {e}")

    async def _call_callback(self, output_type: str, line: str):
        """
        Call the output callback, handling both synchronous and asynchronous callbacks.
        
        Args:
            output_type: The type of output ("stdout" or "stderr")
            line: The output line
        """
        if not self._output_callback:
            return
            
        try:
            result = self._output_callback(output_type, line)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Error in output callback: {e}")

    async def _monitor_process(self):
        """Monitor the process and handle state transitions."""
        try:
            # Wait for the process to complete
            return_code = await self.process.wait()
            
            if self.state != CommandState.CANCELLED:
                if return_code == 0:
                    self.state = CommandState.COMPLETED
                    logger.info("Process completed successfully")
                else:
                    self.state = CommandState.ERROR
                    logger.error(f"Process exited with code {return_code}")
        
        except asyncio.CancelledError:
            logger.debug("Process monitor task cancelled")
            
        except Exception as e:
            logger.error(f"Error monitoring process: {e}")
            self.state = CommandState.ERROR

    async def execute(self, command: List[str], 
                    global_options: List[str] = None,
                    callback: Optional[Callable[[str, str], Any]] = None) -> None:
        """
        Execute an s5cmd command asynchronously.
        
        Args:
            command: List of command arguments to pass to s5cmd
            global_options: List of global options
            callback: Optional callback function that takes (output_type, line) parameters
                    where output_type is "stdout" or "stderr" and line is the output line.
                    The callback can be a coroutine.
                    
        Raises:
            ValueError: If a command is already running or if invalid arguments are provided
            subprocess.SubprocessError: If the command fails to start
        """
        if self.state == CommandState.RUNNING or self.state == CommandState.PAUSED:
            raise ValueError("Another command is already running")
        
        # Validate s5cmd executable path to prevent path traversal
        if not os.path.isabs(self.s5cmd_path):
            # If relative path, ensure it doesn't contain path traversal
            if '..' in self.s5cmd_path or self.s5cmd_path.startswith('/'):
                raise ValueError(f"Invalid s5cmd path: {self.s5cmd_path}")
        
        # List of allowed s5cmd commands
        allowed_commands = ['cp', 'ls', 'rm', 'mb', 'rb', 'du', 'cat', 'version', 'help']
        
        # Validate command is not empty
        if not command:
            raise ValueError("Command list cannot be empty")
        
        # Check that the first argument is a valid s5cmd command
        if command[0] not in allowed_commands:
            raise ValueError(f"Invalid s5cmd command: {command[0]}. Allowed commands: {', '.join(allowed_commands)}")
        
        # Validate all command arguments are strings and don't contain shell metacharacters
        for i, arg in enumerate(command):
            if not isinstance(arg, str):
                raise ValueError(f"Command argument at position {i} must be a string, got {type(arg)}")
            
            # Check for shell metacharacters and other potentially dangerous content
            if any(char in arg for char in ['&', '|', ';', '$', '>', '<', '`', '\\']):
                raise ValueError(f"Command argument contains unsafe characters: {arg}")
            
            # Additional validation for s3 paths
            if arg.startswith('s3://'):
                # Validate s3 path format with wildcard support
                if not re.match(r'^s3://[a-zA-Z0-9._-]+(/[a-zA-Z0-9._-]*)*(/\*)?$', arg):
                    raise ValueError(f"Invalid S3 path format: {arg}")
        
        # Verify s5cmd is available
        await self._verify_s5cmd()

        # Construct the command correctly - s5cmd [global options] COMMAND [command options]
        full_command = [self.s5cmd_path] + (global_options if global_options else []) + command
        logger.info(f"Executing command: {' '.join(full_command)}")
        
        # Reset state
        self.state = CommandState.RUNNING
        self._stopping = False
        self._paused = False
        self._output_callback = callback
        
        try:
            # Command arguments have been extensively validated above to prevent injection
            # nosemgrep: dangerous-asyncio-create-exec-audit
            self.process = await asyncio.create_subprocess_exec(
                *full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                # Use process group on Unix-like systems for better control
                preexec_fn=os.setpgrp if sys.platform != 'win32' else None
            )
            
            # Start tasks to read stdout and stderr
            self._stdout_task = asyncio.create_task(
                self._read_stream(self.process.stdout, False)
            )
            self._stderr_task = asyncio.create_task(
                self._read_stream(self.process.stderr, True)
            )
            self._monitor_task = asyncio.create_task(
                self._monitor_process()
            )
            
        except Exception as e:
            self.state = CommandState.ERROR
            logger.warning(f"Failed to execute command: {e}")
            raise

    async def execute_and_wait(self, command: List[str],
                              global_options: List[str] = None,
                              callback: Optional[Callable[[str, str], Any]] = None) -> Dict[str, Any]:
        """
        Execute an s5cmd command and wait for completion.
        
        Args:
            command: List of command arguments to pass to s5cmd
            global_options: Optional list of global options to pass before the command
            callback: Optional callback function for real-time output
            
        Returns:
            Dict containing execution results:
                - 'success': Boolean indicating if the command succeeded
                - 'return_code': The command's return code
                - 'state': The final state of the command
        """
        await self.execute(command, global_options, callback)
        return await self.wait()

    async def wait(self) -> Dict[str, Any]:
        """
        Wait for the current command to complete and return results.
        
        Returns:
            Dict containing execution results
        """
        if not self.process:
            return {
                'success': False,
                'return_code': -1,
                'state': self.state.value
            }
        
        # Wait for all tasks to complete
        if self._stdout_task and not self._stdout_task.done():
            await self._stdout_task
        
        if self._stderr_task and not self._stderr_task.done():
            await self._stderr_task
        
        if self._monitor_task and not self._monitor_task.done():
            await self._monitor_task
        
        # Get the return code
        return_code = self.process.returncode if self.process else -1
        success = return_code == 0 and self.state != CommandState.CANCELLED
        
        return {
            'success': success,
            'return_code': return_code,
            'state': self.state.value
        }

    async def cancel(self) -> bool:
        """
        Cancel the currently running command.
        
        Returns:
            True if the command was cancelled, False if there was no command to cancel
        """
        if not self.process or self.process.returncode is not None:
            return False
        
        logger.info("Cancelling command execution")
        self.state = CommandState.CANCELLED
        self._stopping = True
        
        # Send signal to the process group
        try:
            if sys.platform != 'win32':
                # On Unix, we can kill the process group
                pgid = os.getpgid(self.process.pid)
                os.killpg(pgid, signal.SIGTERM)
            else:
                # On Windows, just terminate the process
                self.process.terminate()
            
            # Give it a moment to terminate
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                # Force kill if it doesn't terminate
                logger.warning("Process did not terminate gracefully, forcing kill")
                if sys.platform != 'win32':
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                else:
                    self.process.kill()
            
            # Cancel the tasks
            if self._stdout_task and not self._stdout_task.done():
                self._stdout_task.cancel()
            
            if self._stderr_task and not self._stderr_task.done():
                self._stderr_task.cancel()
            
            if self._monitor_task and not self._monitor_task.done():
                self._monitor_task.cancel()
            
            return True
            
        except (ProcessLookupError, PermissionError) as e:
            logger.error(f"Error cancelling process: {e}")
            return False

    async def pause(self) -> bool:
        """
        Pause the currently running command by sending SIGSTOP.
        Note: This only works on Unix-like systems.
        
        Returns:
            True if the command was paused, False otherwise
        """
        if not self.process or self.process.returncode is not None or \
          self.state != CommandState.RUNNING or sys.platform == 'win32':
            return False
        
        logger.info("Pausing command execution")
        try:
            os.kill(self.process.pid, signal.SIGSTOP)
            self._paused = True
            self.state = CommandState.PAUSED
            return True
        except Exception as e:
            logger.error(f"Failed to pause process: {e}")
            return False

    async def resume(self) -> bool:
        """
        Resume a paused command by sending SIGCONT.
        Note: This only works on Unix-like systems.
        
        Returns:
            True if the command was resumed, False otherwise
        """
        if not self.process or self.process.returncode is not None or \
          self.state != CommandState.PAUSED or sys.platform == 'win32':
            return False
        
        logger.info("Resuming command execution")
        try:
            os.kill(self.process.pid, signal.SIGCONT)
            self._paused = False
            self.state = CommandState.RUNNING
            return True
        except Exception as e:
            logger.error(f"Failed to resume process: {e}")
            return False

    def get_state(self) -> str:
        """
        Get the current state of the command.
        
        Returns:
            The current state as a string
        """
        return self.state.value

    async def execute_blocking(self, command: List[str],
                              global_options: List[str] = None,
                              callback: Optional[Callable[[str, str], Any]] = None) -> Dict[str, Any]:
        """
        Execute an s5cmd command and wait for it to complete.
        
        Args:
            command: List of command arguments to pass to s5cmd
            global_options: Optional list of global options to pass before the command
            callback: Optional callback function for real-time output
            
        Returns:
            Dict containing execution results
        """
        await self.execute(command, global_options, callback)
        return await self.wait()