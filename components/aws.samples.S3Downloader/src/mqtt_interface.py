from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Awaitable, Optional

class MQTTInterface(ABC):
    """
    Abstract interface for MQTT communication to ensure compatibility
    between mock implementation and actual AWS IoT Greengrass SDK.
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to the MQTT broker
        
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def disconnect(self) -> bool:
        """
        Disconnect from the MQTT broker
        
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> bool:
        """
        Subscribe to a topic
        
        Args:
            topic: Topic to subscribe to
            callback: Async callback function that receives topic and payload
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic
        
        Args:
            topic: Topic to unsubscribe from
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def publish(self, topic: str, payload: Dict[str, Any]) -> bool:
        """
        Publish a message to a topic
        
        Args:
            topic: Topic to publish to
            payload: Message payload
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def get_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the current state of a device shadow
        
        Args:
            thing_name: Name of the IoT thing
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
            
        Returns:
            Shadow document as a dictionary
        """
        pass
        
    @abstractmethod
    async def update_shadow(self, thing_name: str, state: Dict[str, Any], shadow_name: Optional[str] = None) -> bool:
        """
        Update the state of a device shadow
        
        Args:
            thing_name: Name of the IoT thing
            state: Shadow state to update. Should contain 'reported' and/or 'desired' keys
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def delete_shadow(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """
        Delete a device shadow
        
        Args:
            thing_name: Name of the IoT thing
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def register_shadow_delta_callback(self, thing_name: str, 
                                            callback: Callable[[Dict[str, Any]], Awaitable[None]],
                                            shadow_name: Optional[str] = None) -> bool:
        """
        Register a callback for shadow delta updates
        
        Args:
            thing_name: Name of the IoT thing
            callback: Async callback function that receives the delta payload
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
            
        Returns:
            Success status
        """
        pass
        
    @abstractmethod
    async def unregister_shadow_delta_callback(self, thing_name: str, shadow_name: Optional[str] = None) -> bool:
        """
        Unregister the callback for shadow delta updates
        
        Args:
            thing_name: Name of the IoT thing
            shadow_name: Name of the shadow (None for classic/unnamed shadow)
            
        Returns:
            Success status
        """
        pass