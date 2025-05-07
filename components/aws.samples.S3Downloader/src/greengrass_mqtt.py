import asyncio
import json
from typing import Dict, Any, Callable, Awaitable, Optional
import traceback

from .utils.logging_config import get_logger
from .mqtt_interface import MQTTInterface
from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2

logger = get_logger(__name__)

class GreengrassSDKClient(MQTTInterface):
    """
    Greengrass SDK implementation of the MQTT interface using ClientV2
    """
    
    def __init__(self):
        """Initialize the Greengrass SDK client"""
        self.client = None
        self.subscriptions = {}
        self.connected = False
        self.subscription_operations = {}
        self.event_loop = None
        self.callback_thread = None
        
    async def connect(self) -> bool:
        """
        Connect to the IoT Core via Greengrass
        
        Returns:
            Success status
        """
        logger.info("Connecting to AWS IoT Core via Greengrass")
        
        try:
            # Store the current event loop for callbacks
            self.event_loop = asyncio.get_running_loop()
            
            # Initialize the IPC client v2
            self.client = GreengrassCoreIPCClientV2()
            self.connected = True
            logger.info("Successfully connected to Greengrass Core IPC")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Greengrass Core IPC: {e}")
            logger.error(traceback.format_exc())
            self.connected = False
            return False
        
    async def disconnect(self) -> bool:
        """
        Disconnect from IoT Core
        """
        logger.info("Disconnecting from AWS IoT Core")
        
        try:
            # Cancel all subscription operations
            for operation in self.subscription_operations.values():
                if hasattr(operation, 'close'):
                    operation.close()
                
            # Clear subscription references
            self.subscription_operations.clear()
            self.subscriptions.clear()
            
            # Set client to None
            self.client = None
            self.connected = False
            logger.info("Successfully disconnected from Greengrass Core IPC")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from Greengrass Core IPC: {e}")
            logger.error(traceback.format_exc())
            return False
        
    async def subscribe(self, topic: str, callback: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> bool:
        """
        Subscribe to a topic using Greengrass SDK
        
        Args:
            topic: Topic to subscribe to
            callback: Async callback function
        """
        logger.info(f"Subscribing to {topic}")
        
        # Store the callback and the event loop
        self.subscriptions[topic] = callback
        
        try:
            # Define handlers for stream events
            def on_stream_event(event):
                try:
                    # Parse the payload
                    message = str(event.message.payload, 'utf-8')
                    logger.info(f'Received message on {topic}: {message}')
                    payload = json.loads(message)

                    # Log if commandId is in the payload
                    if 'commandId' in payload:
                        logger.info(f"Command ID in received message: {payload['commandId']}")
                    else:
                        logger.warning(f"No commandId in received message: {json.dumps(payload)}")

                    # Schedule the async callback to run in the event loop
                    if self.event_loop and self.event_loop.is_running():
                        self.event_loop.call_soon_threadsafe(
                            lambda: asyncio.create_task(
                                self._run_callback(topic, payload)
                            )
                        )
                    else:
                        logger.error(f"Cannot process message - no running event loop")
                except Exception as e:
                    logger.error(f"Error processing message from {topic}: {e}")
                    logger.error(traceback.format_exc())
            
            # Error handler
            def on_stream_error(error):
                logger.error(f"Subscription error on {topic}: {error}")
                return False  # Keep stream open
            
            # Closed handler
            def on_stream_closed():
                logger.info(f"Subscription to {topic} closed")
            
            # Subscribe to the IoT Core topic using ClientV2 API
            # The operation returns a tuple with (subscription, operation)
            _, operation = self.client.subscribe_to_iot_core(
                topic_name=topic,
                qos=1,
                on_stream_event=on_stream_event,
                on_stream_error=on_stream_error,
                on_stream_closed=on_stream_closed
            )
            
            # Store the operation for later cleanup
            self.subscription_operations[topic] = operation
            
            logger.info(f"Successfully subscribed to {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe to {topic}: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _run_callback(self, topic: str, payload: Dict[str, Any]) -> None:
        """
        Run the callback function for a topic with error handling
        
        Args:
            topic: The topic the message was received on
            payload: The message payload
        """
        try:
            callback = self.subscriptions.get(topic)
            if callback:
                await callback(topic, payload)
        except Exception as e:
            logger.error(f"Error in callback for {topic}: {e}")
            logger.error(traceback.format_exc())
        
    async def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic
        
        Args:
            topic: Topic to unsubscribe from
        """
        logger.info(f"Unsubscribing from {topic}")
        
        if topic in self.subscriptions:
            # Remove callback
            del self.subscriptions[topic]
            
            try:
                # Close the subscription operation
                if topic in self.subscription_operations:
                    self.subscription_operations[topic].close()
                    del self.subscription_operations[topic]
                    
                logger.info(f"Successfully unsubscribed from {topic}")
                return True
            except Exception as e:
                logger.error(f"Failed to unsubscribe from {topic}: {e}")
                logger.error(traceback.format_exc())
                return False
                
        return False
        
    async def publish(self, topic: str, payload: Dict[str, Any]) -> bool:
        """
        Publish a message to a topic
        
        Args:
            topic: Topic to publish to
            payload: Message payload
        """
        if not self.connected:
            logger.error("Cannot publish, not connected")
            return False
            
        logger.info(f"Publishing to {topic}")
        
        try:
            # Convert the payload to JSON string, then to bytes
            payload_json = json.dumps(payload)
            payload_bytes = payload_json.encode('utf-8')
            
            # Publish to IoT Core topic using ClientV2 API
            self.client.publish_to_iot_core(
                topic_name=topic,
                qos=1,
                payload=payload_bytes
            )
            
            logger.info(f"Successfully published to {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            logger.error(traceback.format_exc())
            return False
        
    def _get_shadow_topic(self, thing_name: str, operation: str, shadow_name: Optional[str] = None) -> str:
        """
        Get the MQTT topic for shadow operations
        
        Args:
            thing_name: Name of the IoT thing
            operation: Shadow operation (get, update, delete)
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
        """
        if shadow_name:
            return f"$aws/things/{thing_name}/shadow/name/{shadow_name}/{operation}"
        return f"$aws/things/{thing_name}/shadow/{operation}"
    
    async def get_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> Dict[str, Any]:
        """Get the current state of a device shadow"""
        if not self.connected:
            logger.error("Cannot get shadow, not connected")
            return {}
            
        logger.info(f"Getting shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        # Create a future to store the result
        result_future = self.event_loop.create_future()
        
        # Create the callback function
        async def shadow_response_callback(topic: str, payload: Dict[str, Any]) -> None:
            # Check if this is an accepted or rejected response
            if "/accepted" in topic and not result_future.done():
                result_future.set_result(payload)
            elif "/rejected" in topic and not result_future.done():
                logger.error(f"Shadow get rejected: {json.dumps(payload)}")
                result_future.set_exception(Exception(f"Shadow get rejected: {json.dumps(payload)}"))
        
        # Get topic names
        get_topic = self._get_shadow_topic(thing_name, "get", shadow_name)
        accepted_topic = f"{get_topic}/accepted"
        rejected_topic = f"{get_topic}/rejected"
        
        # Subscribe to the response topics
        await self.subscribe(accepted_topic, shadow_response_callback)
        await self.subscribe(rejected_topic, shadow_response_callback)
        
        try:
            # Publish empty message to get shadow
            await self.publish(get_topic, {})
            
            # Wait for the response with timeout
            return await asyncio.wait_for(result_future, timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for shadow response")
            return {}
        except Exception as e:
            logger.error(f"Error getting shadow: {e}")
            return {}
        finally:
            # Unsubscribe from response topics
            await self.unsubscribe(accepted_topic)
            await self.unsubscribe(rejected_topic)
    
    async def update_shadow(self, thing_name: str, state: Dict[str, Any], shadow_name: Optional[str] = None) -> bool:
        """Update the state of a device shadow"""
        if not self.connected:
            logger.error("Cannot update shadow, not connected")
            return False
            
        logger.info(f"Updating shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        # Create a future for the result
        result_future = self.event_loop.create_future()
        
        # Create the callback function
        async def shadow_response_callback(topic: str, payload: Dict[str, Any]) -> None:
            # Check if this is an accepted or rejected response
            if "/accepted" in topic and not result_future.done():
                result_future.set_result(True)
            elif "/rejected" in topic and not result_future.done():
                logger.error(f"Shadow update rejected: {json.dumps(payload)}")
                result_future.set_exception(Exception(f"Shadow update rejected: {json.dumps(payload)}"))
        
        # Get topic names
        update_topic = self._get_shadow_topic(thing_name, "update", shadow_name)
        accepted_topic = f"{update_topic}/accepted"
        rejected_topic = f"{update_topic}/rejected"
        
        # Subscribe to the response topics
        await self.subscribe(accepted_topic, shadow_response_callback)
        await self.subscribe(rejected_topic, shadow_response_callback)
        
        try:
            # Publish update
            shadow_document = {"state": state}
            await self.publish(update_topic, shadow_document)
            
            # Wait for the response with timeout
            return await asyncio.wait_for(result_future, timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for shadow update response")
            return False
        except Exception as e:
            logger.error(f"Error updating shadow: {e}")
            return False
        finally:
            # Unsubscribe from response topics
            await self.unsubscribe(accepted_topic)
            await self.unsubscribe(rejected_topic)
    
    async def delete_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """Delete a device shadow"""
        if not self.connected:
            logger.error("Cannot delete shadow, not connected")
            return False
            
        logger.info(f"Deleting shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        # Create a future for the result
        result_future = self.event_loop.create_future()
        
        # Create the callback function
        async def shadow_response_callback(topic: str, payload: Dict[str, Any]) -> None:
            # Check if this is an accepted or rejected response
            if "/accepted" in topic and not result_future.done():
                result_future.set_result(True)
            elif "/rejected" in topic and not result_future.done():
                logger.error(f"Shadow delete rejected: {json.dumps(payload)}")
                result_future.set_exception(Exception(f"Shadow delete rejected: {json.dumps(payload)}"))
        
        # Get topic names
        delete_topic = self._get_shadow_topic(thing_name, "delete", shadow_name)
        accepted_topic = f"{delete_topic}/accepted"
        rejected_topic = f"{delete_topic}/rejected"
        
        # Subscribe to the response topics
        await self.subscribe(accepted_topic, shadow_response_callback)
        await self.subscribe(rejected_topic, shadow_response_callback)
        
        try:
            # Publish delete request
            await self.publish(delete_topic, {})
            
            # Wait for the response with timeout
            return await asyncio.wait_for(result_future, timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for shadow delete response")
            return False
        except Exception as e:
            logger.error(f"Error deleting shadow: {e}")
            return False
        finally:
            # Unsubscribe from response topics
            await self.unsubscribe(accepted_topic)
            await self.unsubscribe(rejected_topic)
    
    async def register_shadow_delta_callback(self, thing_name: str, 
                                           callback: Callable[[Dict[str, Any]], Awaitable[None]],
                                           shadow_name: Optional[str] = None) -> bool:
        """Register a callback for shadow delta updates"""
        if not self.connected:
            logger.error("Cannot register shadow delta callback, not connected")
            return False
            
        logger.info(f"Registering shadow delta callback for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        # Create a wrapper callback that extracts the payload
        async def delta_wrapper(topic: str, payload: Dict[str, Any]) -> None:
            try:
                await callback(payload)
            except Exception as e:
                logger.error(f"Error in shadow delta callback: {e}")
                logger.error(traceback.format_exc())
        
        # Subscribe to the delta topic
        if shadow_name:
            delta_topic = f"$aws/things/{thing_name}/shadow/name/{shadow_name}/update/delta"
        else:
            delta_topic = f"$aws/things/{thing_name}/shadow/update/delta"
            
        return await self.subscribe(delta_topic, delta_wrapper)
    
    async def unregister_shadow_delta_callback(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """Unregister the callback for shadow delta updates"""
        if not self.connected:
            logger.error("Cannot unregister shadow delta callback, not connected")
            return False
            
        logger.info(f"Unregistering shadow delta callback for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        # Unsubscribe from the delta topic
        if shadow_name:
            delta_topic = f"$aws/things/{thing_name}/shadow/name/{shadow_name}/update/delta"
        else:
            delta_topic = f"$aws/things/{thing_name}/shadow/update/delta"
            
        return await self.unsubscribe(delta_topic)