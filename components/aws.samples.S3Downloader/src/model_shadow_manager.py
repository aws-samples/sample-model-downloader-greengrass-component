import asyncio
import json
import time
from typing import Dict, Any, Optional
import os
import uuid

from .utils.logging_config import get_logger
from .mqtt_interface import MQTTInterface

logger = get_logger(__name__)

class ModelShadowManager:
    """
    Manages model metadata using AWS IoT Device Shadows with simplified metadata tracking
    """
    def __init__(self, mqtt_client: MQTTInterface, device_id: str):
        """
        Initialize the ModelShadowManager
        
        Args:
            mqtt_client: MQTT client implementing the extended MQTTInterface
            device_id: Device identifier
        """
        self.mqtt_client = mqtt_client
        self.device_id = device_id
        self.shadow_name = "models"  # Using a named shadow for all models
        
        # Local cache of model metadata
        self.models_cache: Dict[str, Dict[str, Any]] = {}
        
        # Flag to track if we've initialized from shadow
        self.initialized = False
        
    async def initialize(self) -> bool:
        """
        Initialize by retrieving current shadow state
        
        Returns:
            Success status
        """
        logger.info(f"Initializing ModelShadowManager for device {self.device_id}")
        
        try:
            # Get the current shadow document
            shadow_doc = await self.mqtt_client.get_shadow(self.device_id, self.shadow_name)
            
            if shadow_doc and 'state' in shadow_doc and 'reported' in shadow_doc['state']:
                # Extract models from the shadow document
                reported_models = shadow_doc['state']['reported'].get('models', {})
                self.models_cache = reported_models
                logger.info(f"Loaded {len(reported_models)} models from device shadow")
            else:
                # Initialize empty shadow if it doesn't exist
                logger.info("No existing model shadow found, initializing empty shadow")
                await self.update_shadow()
                
            # Register for shadow delta callbacks
            await self.mqtt_client.register_shadow_delta_callback(
                self.device_id, 
                self._handle_shadow_delta,
                self.shadow_name
            )
            
            self.initialized = True
            return True
            
        except Exception as e:
            logger.error(f"Error initializing ModelShadowManager: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    async def _handle_shadow_delta(self, delta_payload: Dict[str, Any]) -> None:
        """
        Handle shadow delta updates
        
        Args:
            delta_payload: Delta payload from AWS IoT
        """
        logger.info(f"Received shadow delta: {json.dumps(delta_payload)}")
        
        if 'state' not in delta_payload:
            logger.warning("No state found in delta payload")
            return
            
        # Extract delta models
        delta_models = delta_payload['state'].get('models', {})
        if not delta_models:
            return
            
        # Update our local cache with the delta changes
        for model_id, model_data in delta_models.items():
            if model_data is None:
                # Remove model if the delta contains null
                if model_id in self.models_cache:
                    logger.info(f"Removing model {model_id} based on delta")
                    del self.models_cache[model_id]
            else:
                # Update or add model
                if model_id in self.models_cache:
                    self.models_cache[model_id].update(model_data)
                    logger.info(f"Updated model {model_id} based on delta")
                else:
                    self.models_cache[model_id] = model_data
                    logger.info(f"Added new model {model_id} based on delta")
                    
        # Report our updated state back to the shadow
        await self.update_shadow()
        
    async def update_shadow(self) -> bool:
        """
        Update the device shadow with current model metadata
        
        Returns:
            Success status
        """
        try:
            # Prepare shadow state document
            state = {
                "reported": {
                    "models": self.models_cache
                }
            }
            
            # Update the shadow
            result = await self.mqtt_client.update_shadow(
                self.device_id, 
                state, 
                self.shadow_name
            )
            
            return result
        except Exception as e:
            logger.error(f"Error updating model shadow: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    async def add_or_update_model(self, model_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a new model or update an existing one
        
        Args:
            model_data: Model metadata
                {
                    'model_id': 'Qwen2.5-VL-7B-Instruct-AWQ',  # Required: Model identifier
                    'local_path': '/models/qwen/model.bin',     # Required: Local filesystem path
                    'model_name': 'Qwen VL',                    # Optional: Human-readable name
                    'model_version': '2.5',                     # Optional: Version
                    'last_updated': 1682145600,                 # Optional: Timestamp
                    ... any other fields ...
                }
                
        Returns:
            Updated model metadata
        """
        # Ensure the models cache is initialized
        if not self.initialized:
            success = await self.initialize()
            if not success:
                return {'success': False, 'error': 'Failed to initialize shadow manager'}
        
        # Validate required fields
        required_fields = ['model_id', 'local_path']
        missing_fields = [f for f in required_fields if f not in model_data]
        
        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
            
        model_id = model_data['model_id']
        
        # Add timestamp metadata if not provided
        if 'last_updated' not in model_data:
            model_data['last_updated'] = time.time()
            
        # Update our local cache
        if model_id in self.models_cache:
            self.models_cache[model_id].update(model_data)
        else:
            self.models_cache[model_id] = model_data
            
        # Update the shadow
        success = await self.update_shadow()
        
        if not success:
            return {
                'success': False, 
                'error': 'Failed to update shadow',
                'model_id': model_id
            }
            
        return {
            'success': True,
            'model_id': model_id,
            'model': self.models_cache[model_id]
        }
        
    async def get_model(self, model_id: str) -> Dict[str, Any]:
        """
        Get metadata for a specific model
        
        Args:
            model_id: Model identifier (e.g., 'Qwen2.5-VL-7B-Instruct-AWQ')
            
        Returns:
            Model metadata or error
        """
        # Ensure the models cache is initialized
        if not self.initialized:
            success = await self.initialize()
            if not success:
                return {'success': False, 'error': 'Failed to initialize shadow manager'}
                
        if model_id not in self.models_cache:
            return {
                'success': False,
                'error': f'Model ID {model_id} not found'
            }
            
        return {
            'success': True,
            'model_id': model_id,
            'model': self.models_cache[model_id]
        }
        
    async def get_all_models(self) -> Dict[str, Any]:
        """
        Get metadata for all models
        
        Returns:
            Dictionary with all model metadata
        """
        # Ensure the models cache is initialized
        if not self.initialized:
            success = await self.initialize()
            if not success:
                return {'success': False, 'error': 'Failed to initialize shadow manager'}
                
        return {
            'success': True,
            'models': self.models_cache
        }
        
    async def delete_model(self, model_id: str) -> Dict[str, Any]:
        """
        Delete a model from the shadow
        
        Args:
            model_id: Model identifier
            
        Returns:
            Success status
        """
        # Ensure the models cache is initialized
        if not self.initialized:
            success = await self.initialize()
            if not success:
                return {'success': False, 'error': 'Failed to initialize shadow manager'}
                
        if model_id not in self.models_cache:
            return {
                'success': False,
                'error': f'Model ID {model_id} not found'
            }
            
        # Remove from local cache
        del self.models_cache[model_id]
        
        # Update the shadow
        success = await self.update_shadow()
        
        if not success:
            return {
                'success': False, 
                'error': 'Failed to update shadow after deletion',
                'model_id': model_id
            }
            
        return {
            'success': True,
            'message': f'Model {model_id} deleted successfully'
        }