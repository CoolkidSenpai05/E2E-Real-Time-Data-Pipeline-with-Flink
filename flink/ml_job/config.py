# flink/ml_job/config.py
import os
import json
import pickle
import logging

logger = logging.getLogger(__name__)

class Config:
    """Cấu hình cho Flink ML Job"""
    
    def __init__(self):
        # Kafka configuration
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'broker:29092')
        self.input_topic = os.getenv('INPUT_TOPIC', 'clickstream')
        self.output_topic = os.getenv('OUTPUT_TOPIC', 'clickstream_labeled')
        self.alert_topic = os.getenv('ALERT_TOPIC', 'fraud_alerts')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'flink-fraud-detection')
        
        # Model configuration
        self.model_path = os.getenv('MODEL_PATH', '/opt/flink/ml_job/random_forest_clickstream.onnx')
        # self.scaler_path = os.getenv('SCALER_PATH', '/opt/flink/ml_job/scaler.pkl')
        self.scaler_path = None  # Không sử dụng scaler trong phiên bản này
        self.feature_columns_path = os.getenv('FEATURE_COLUMNS_PATH', '/opt/flink/ml_job/feature_columns.pkl')
        self.metadata_path = os.getenv('METADATA_PATH', '/opt/flink/ml_job/model_metadata.json')
        
        # Inference configuration
        self.fraud_threshold = float(os.getenv('FRAUD_THRESHOLD', '0.85'))
        self.checkpoint_interval = int(os.getenv('CHECKPOINT_INTERVAL', '60000'))  # 60 giây
        self.parallelism = int(os.getenv('PARALLELISM', '1'))
        
        # Load model metadata
        self.load_metadata()
    
    def load_metadata(self):
        """Load model metadata"""
        try:
            with open(self.metadata_path, 'r') as f:
                self.model_metadata = json.load(f)
            logger.info(f"Loaded model metadata: {self.model_metadata.get('model_type')}")
            logger.info(f"Features: {self.model_metadata.get('num_features')}")
        except Exception as e:
            logger.warning(f"Could not load metadata: {e}")
            self.model_metadata = {}
    
    # def load_feature_columns(self):
    #     """Load list of feature columns"""
    #     with open(self.feature_columns_path, 'rb') as f:
    #         return pickle.load(f)

    def load_feature_columns(self):
        """Load list of feature columns"""
        try:
            if not os.path.exists(self.feature_columns_path):
                logger.error(f"❌ Feature columns file NOT FOUND: {self.feature_columns_path}")
                raise FileNotFoundError(f"Feature columns file not found at {self.feature_columns_path}")
        
            with open(self.feature_columns_path, 'rb') as f:
                columns = pickle.load(f)
            logger.info(f"✅ Loaded {len(columns)} feature columns")
            return columns
        except Exception as e:
            logger.error(f"❌ Failed to load feature columns: {e}")
            raise
    
    def load_scaler(self):
        """Load StandardScaler if exists"""
        try:
            with open(self.scaler_path, 'rb') as f:
                return pickle.load(f)
        except:
            return None

# Global config instance
config = Config()