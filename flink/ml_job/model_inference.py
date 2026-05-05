# flink/ml_job/model_inference.py
import onnxruntime as rt
import numpy as np
import logging
from config import config

logger = logging.getLogger(__name__)

class FraudDetector:
    """Fraud detection model using ONNX Runtime"""
    
    def __init__(self):
        self.session = None
        self.input_name = None
        self.output_names = None
        self.load_model()
    
    def load_model(self):
        """Load ONNX model"""
        try:
            # Tạo inference session với CPU provider
            self.session = rt.InferenceSession(
                config.model_path,
                providers=['CPUExecutionProvider']
            )
            
            self.input_name = self.session.get_inputs()[0].name
            self.output_names = [output.name for output in self.session.get_outputs()]
            
            logger.info(f"Model loaded successfully from {config.model_path}")
            logger.info(f"Input name: {self.input_name}")
            logger.info(f"Output names: {self.output_names}")
            
            # Get correct number of features from model metadata
            num_features = config.model_metadata.get('input_schema', {}).get('num_features', 21)
            logger.info(f"Expected number of features: {num_features}")
            
            # Warm up model
            dummy_input = np.random.randn(1, num_features).astype(np.float32)
            self.session.run(None, {self.input_name: dummy_input})
            logger.info("Model warm-up completed")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def predict(self, features):
        """
        Predict fraud probability
        
        Args:
            features: numpy array of shape (1, num_features)
            
        Returns:
            dict with prediction results
        """
        try:
            # Validate input shape
            if features.shape[1] != 21:
                logger.error(f"❌ INVALID INPUT SHAPE: Expected (1, 21) but got {features.shape}")
                raise ValueError(f"Expected 21 features but got {features.shape[1]}")
            
            # Chạy inference
            outputs = self.session.run(None, {self.input_name: features})
            
            # Lấy kết quả
            prediction = int(outputs[0][0]) if len(outputs) > 0 else 0
            probabilities = outputs[1][0] if len(outputs) > 1 else [1-prediction, prediction]
            
            fraud_probability = float(probabilities[1])  # Probability of fraud
            is_fraud = fraud_probability >= config.fraud_threshold
            
            return {
                'is_fraud': is_fraud,
                'fraud_probability': fraud_probability,
                'prediction': prediction,
                'threshold': config.fraud_threshold
            }
            
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {
                'is_fraud': False,
                'fraud_probability': 0.0,
                'prediction': 0,
                'error': str(e)
            }
    
    def health_check(self):
        """Check if model is ready"""
        try:
            features = np.random.randn(1, config.model_metadata.get('num_features', 21)).astype(np.float32)
            self.session.run(None, {self.input_name: features})
            return True
        except:
            return False