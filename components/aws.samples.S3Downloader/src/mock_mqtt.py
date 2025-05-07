import asyncio
import json
import time
from typing import Dict, Any, Callable, Awaitable, List, Optional

from .utils.logging_config import get_logger
from .mqtt_interface import MQTTInterface

logger = get_logger(__name__)


class MockMQTTClient(MQTTInterface):
    """
    Mock implementation of the MQTT interface for local testing
    """
    
    def __init__(self):
        self.subscriptions: Dict[str, List[Callable]] = {}
        self.connected = False
        self.command_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.shadows: Dict[str, Dict[str, Any]] = {}
        self.shadow_delta_callbacks: Dict[str, Callable] = {}
        
    async def connect(self) -> bool:
        """Connect to the mock MQTT broker"""
        logger.info("Mock MQTT: Connected")
        self.connected = True
        return True
        
    async def disconnect(self) -> bool:
        """Disconnect from the mock MQTT broker"""
        logger.info("Mock MQTT: Disconnected")
        self.connected = False
        return True
        
    async def subscribe(self, topic: str, callback: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> bool:
        """Subscribe to a topic"""
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        
        self.subscriptions[topic].append(callback)
        logger.info(f"Mock MQTT: Subscribed to {topic}")
        return True
        
    async def unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from a topic"""
        if topic in self.subscriptions:
            del self.subscriptions[topic]
            logger.info(f"Mock MQTT: Unsubscribed from {topic}")
            return True
        return False
        
    async def publish(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Publish a message to a topic"""
        if not self.connected:
            logger.error("Mock MQTT: Cannot publish, not connected")
            return False
            
        logger.info(f"Mock MQTT: Publishing to {topic}: {json.dumps(payload)}")
        
        # Put the message in the response queue for interactive testing
        await self.response_queue.put((topic, payload))
        
        # Deliver to subscribers if any
        if topic in self.subscriptions:
            for callback in self.subscriptions[topic]:
                try:
                    await callback(topic, payload)
                except Exception as e:
                    logger.error(f"Error in subscriber callback: {e}")
        
        return True
        
    async def inject_message(self, topic: str, payload: Dict[str, Any]) -> None:
        """
        Inject a message as if it came from the broker
        
        Args:
            topic: Topic to publish to
            payload: Message payload
        """
        logger.info(f"Injecting message to {topic}: {json.dumps(payload)}")
        
        if topic in self.subscriptions:
            for callback in self.subscriptions[topic]:
                try:
                    await callback(topic, payload)
                    logger.info("Callback executed successfully")
                except Exception as e:
                    logger.error(f"Error in subscriber callback: {e}")
        else:
            logger.warning(f"No subscribers found for topic {topic}")
        
    async def get_next_response(self) -> tuple:
        """
        Get the next published response from the queue
        
        Returns:
            Tuple of (topic, payload)
        """
        return await self.response_queue.get()
        
    async def wait_for_command(self) -> Dict[str, Any]:
        """
        Wait for a command to be injected
        
        Returns:
            Command dictionary
        """
        return await self.command_queue.get()
        
    async def inject_command(self, command: Dict[str, Any]) -> None:
        """
        Inject a command for testing
        
        Args:
            command: Command dictionary
        """
        await self.command_queue.put(command)

    def _get_shadow_key(self, thing_name: str, shadow_name: Optional[str] = None) -> str:
        """Get a key for shadow storage based on thing name and shadow name"""
        if shadow_name:
            return f"{thing_name}:{shadow_name}"
        return thing_name
    
    def _initialize_shadow(self, shadow_key: str) -> None:
        """Initialize a shadow if it doesn't exist"""
        if shadow_key not in self.shadows:
            self.shadows[shadow_key] = {
                "state": {
                    "reported": {},
                    "desired": {}
                },
                "metadata": {
                    "reported": {},
                    "desired": {}
                },
                "version": 1,
                "timestamp": int(time.time())
            }
    
    async def get_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> Dict[str, Any]:
        """Get the current state of a device shadow"""
        logger.info(f"Mock MQTT: Getting shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        shadow_key = self._get_shadow_key(thing_name, shadow_name)
        self._initialize_shadow(shadow_key)
        
        return self.shadows[shadow_key]
    
    async def update_shadow(self, thing_name: str, state: Dict[str, Any], shadow_name: Optional[str] = None) -> bool:
        """Update the state of a device shadow"""
        logger.info(f"Mock MQTT: Updating shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}: {json.dumps(state)}")
        
        shadow_key = self._get_shadow_key(thing_name, shadow_name)
        self._initialize_shadow(shadow_key)
        
        shadow = self.shadows[shadow_key]
        
        # Update version and timestamp
        shadow["version"] += 1
        shadow["timestamp"] = int(time.time())
        
        # Update state
        if "reported" in state:
            shadow["state"]["reported"].update(state["reported"])
        if "desired" in state:
            shadow["state"]["desired"].update(state["desired"])
            
            # Check for deltas between reported and desired
            delta = {}
            for key, value in shadow["state"]["desired"].items():
                if key not in shadow["state"]["reported"] or shadow["state"]["reported"][key] != value:
                    delta[key] = value
            
            # Trigger delta callback if there are differences
            if delta and shadow_key in self.shadow_delta_callbacks:
                delta_payload = {
                    "state": delta,
                    "metadata": {
                        # Simplified metadata
                        key: {"timestamp": shadow["timestamp"]} for key in delta
                    },
                    "version": shadow["version"],
                    "timestamp": shadow["timestamp"]
                }
                
                try:
                    await self.shadow_delta_callbacks[shadow_key](delta_payload)
                except Exception as e:
                    logger.error(f"Error in shadow delta callback: {e}")
        
        return True
    
    async def delete_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """Delete a device shadow"""
        logger.info(f"Mock MQTT: Deleting shadow for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        shadow_key = self._get_shadow_key(thing_name, shadow_name)
        
        if shadow_key in self.shadows:
            del self.shadows[shadow_key]
            return True
        
        return False
    
    async def register_shadow_delta_callback(self, thing_name: str, 
                                            callback: Callable[[Dict[str, Any]], Awaitable[None]],
                                            shadow_name: Optional[str] = None) -> bool:
        """Register a callback for shadow delta updates"""
        logger.info(f"Mock MQTT: Registering shadow delta callback for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        shadow_key = self._get_shadow_key(thing_name, shadow_name)
        self.shadow_delta_callbacks[shadow_key] = callback
        
        return True
    
    async def unregister_shadow_delta_callback(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """Unregister the callback for shadow delta updates"""
        logger.info(f"Mock MQTT: Unregistering shadow delta callback for {thing_name}{f'/{shadow_name}' if shadow_name else ''}")
        
        shadow_key = self._get_shadow_key(thing_name, shadow_name)
        
        if shadow_key in self.shadow_delta_callbacks:
            del self.shadow_delta_callbacks[shadow_key]
            return True
        
        return False