# flink/ml_job/feature_engineering.py
import numpy as np
import pandas as pd
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

class FeatureExtractor:
    """Extract features from raw clickstream events"""
    
    def __init__(self, feature_columns, scaler=None):
        self.feature_columns = feature_columns
        self.scaler = scaler
        logger.info(f"Initialized FeatureExtractor with {len(feature_columns)} features")
    
    def extract_features(self, event_json):
        """
        Extract features from a single clickstream event
        
        Args:
            event_json: JSON string or dict of the event
            
        Returns:
            numpy array of features
        """
        if isinstance(event_json, str):
            event = json.loads(event_json)
        else:
            event = event_json
        
        # Parse timestamp
        timestamp = event.get('timestamp', datetime.now().isoformat())
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            dt = datetime.now()
        
        # Extract geo_location if nested
        geo_lat = 0.0
        geo_lon = 0.0
        if 'geo_location' in event:
            geo = event['geo_location']
            if isinstance(geo, str):
                geo = json.loads(geo)
            geo_lat = float(geo.get('lat', 0))
            geo_lon = float(geo.get('lon', 0))
        
        # Basic event properties
        event_type = event.get('event_type', 'page_view')
        device = event.get('device', 'desktop')
        url = event.get('url', '')
        purchase_amount = event.get('purchase_amount', 0) or 0
        
        # Build features dictionary
        features = {
            # Time features
            'hour': dt.hour,
            'day_of_week': dt.weekday(),
            'is_weekend': 1 if dt.weekday() in [5, 6] else 0,
            'is_night': 1 if 0 <= dt.hour < 5 else 0,
            
            # URL features
            'url_length': len(url),
            'url_depth': url.count('/'),
            'has_suspicious_path': 1 if any(x in url.lower() for x in 
                ['admin', 'login', 'config', 'wp-', '.php', '.env', '.git']) else 0,
            
            # Purchase features
            'purchase_amount': float(purchase_amount),
            'is_high_value': 1 if float(purchase_amount) > 200 else 0,
            'log_purchase_amount': np.log1p(float(purchase_amount)),
            
            # Event type dummies
            'event_page_view': 1 if event_type == 'page_view' else 0,
            'event_add_to_cart': 1 if event_type == 'add_to_cart' else 0,
            'event_purchase': 1 if event_type == 'purchase' else 0,
            'event_logout': 1 if event_type == 'logout' else 0,
            
            # Device dummies
            'device_mobile': 1 if device == 'mobile' else 0,
            'device_desktop': 1 if device == 'desktop' else 0,
            'device_tablet': 1 if device == 'tablet' else 0,
            
            # Interaction features
            'night_mobile': (1 if 0 <= dt.hour < 5 else 0) * (1 if device == 'mobile' else 0),
            'night_high_value': (1 if 0 <= dt.hour < 5 else 0) * (1 if float(purchase_amount) > 200 else 0),
            'mobile_high_value': (1 if device == 'mobile' else 0) * (1 if float(purchase_amount) > 200 else 0),
            
            # Ratio features
            'amount_per_url_depth': float(purchase_amount) / (url.count('/') + 1),
        }
        
        # Select only the features used in training
        feature_vector = []
        for col in self.feature_columns:
            if col in features:
                feature_vector.append(features[col])
            else:
                feature_vector.append(0.0)
                logger.warning(f"Feature {col} not found, using 0")
        
        # Log feature vector size for debugging
        logger.debug(f"Feature columns count: {len(self.feature_columns)}")
        logger.debug(f"Feature vector size: {len(feature_vector)}")
        if len(feature_vector) != len(self.feature_columns):
            logger.error(f"❌ MISMATCH: Expected {len(self.feature_columns)} features but got {len(feature_vector)}")
        
        feature_array = np.array(feature_vector, dtype=np.float32).reshape(1, -1)
        
        # Final check before returning
        if feature_array.shape[1] != len(self.feature_columns):
            logger.error(f"❌ ARRAY SHAPE MISMATCH: Array has shape {feature_array.shape} but expected shape (1, {len(self.feature_columns)})")
        
        # Apply scaling if scaler is available
        if self.scaler is not None:
            try:
                feature_array = self.scaler.transform(feature_array)
            except Exception as e:
                logger.error(f"Scaling error: {e}")
        
        return feature_array