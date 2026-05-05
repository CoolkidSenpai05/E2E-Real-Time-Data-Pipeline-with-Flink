# flink/ml_job/main.py
import os
import sys
import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.watermark_strategy import WatermarkStrategy

from config import config
from feature_engineering import FeatureExtractor
from model_inference import FraudDetector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FraudDetectionMapFunction(MapFunction):
    """Map function for real-time fraud detection"""
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize model and feature extractor"""
        logger.info("Initializing FraudDetectionMapFunction...")
        
        # Load model metadata
        self.feature_columns = config.load_feature_columns()
        self.scaler = config.load_scaler()
        
        logger.info(f"✅ Loaded feature columns: {len(self.feature_columns)} features")
        logger.info(f"   Features: {self.feature_columns}")
        
        # Initialize components
        self.feature_extractor = FeatureExtractor(self.feature_columns, self.scaler)
        self.detector = FraudDetector()
        
        # Metrics
        self.processed_count = 0
        self.fraud_count = 0
        self.error_count = 0
        
        logger.info(f"Initialization complete. Features: {len(self.feature_columns)}")
        logger.info(f"Fraud threshold: {config.fraud_threshold}")
    
    def map(self, value: str) -> str:
        """
        Process each clickstream event
        
        Args:
            value: JSON string of clickstream event
            
        Returns:
            JSON string with fraud prediction added
        """
        try:
            # Parse input
            event = json.loads(value)
            event_id = event.get('event_id', 'unknown')
            
            # Extract features
            features = self.feature_extractor.extract_features(event)
            
            # DEBUG: Log feature shape on first event
            if self.processed_count == 0:
                logger.info(f"🔍 DEBUG: First event feature shape: {features.shape}")
                logger.info(f"🔍 DEBUG: Expected shape: (1, {len(self.feature_columns)})")
            
            # Predict
            prediction = self.detector.predict(features)
            
            # Update event with prediction
            event['fraud_prediction'] = prediction['is_fraud']
            event['fraud_probability'] = prediction['fraud_probability']
            event['prediction_timestamp'] = datetime.now().isoformat()
            event['model_version'] = config.model_metadata.get('model_type', 'unknown')
            
            # Update metrics
            self.processed_count += 1
            if prediction['is_fraud']:
                self.fraud_count += 1
                logger.warning(f"FRAUD DETECTED - Event: {event_id}, Probability: {prediction['fraud_probability']:.3f}")
            
            # Log metrics periodically
            if self.processed_count % 100 == 0:
                logger.info(f"Processed: {self.processed_count}, Fraud: {self.fraud_count}, Errors: {self.error_count}")
            
            return json.dumps(event)
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing event: {e}")
            
            # Return original event with error flag
            try:
                event = json.loads(value)
                event['fraud_prediction'] = False
                event['fraud_probability'] = 0.0
                event['processing_error'] = str(e)
                return json.dumps(event)
            except:
                return value

def create_kafka_source():
    """Create Kafka source for clickstream events"""
    return KafkaSource.builder() \
        .set_bootstrap_servers(config.kafka_broker) \
        .set_topics(config.input_topic) \
        .set_group_id(config.consumer_group) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

def create_kafka_sink(topic):
    """Create Kafka sink for processed events"""
    return KafkaSink.builder() \
        .set_bootstrap_servers(config.kafka_broker) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

def main():
    """Main Flink job"""
    logger.info("Starting Flink Fraud Detection Job")
    logger.info(f"Kafka Broker: {config.kafka_broker}")
    logger.info(f"Input Topic: {config.input_topic}")
    logger.info(f"Output Topic: {config.output_topic}")
    
    try:
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        logger.info("Created Flink execution environment")
        
        # Configure checkpointing for fault tolerance
        env.enable_checkpointing(config.checkpoint_interval)
        env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
        env.get_checkpoint_config().set_checkpoint_timeout(60000)
        logger.info("Checkpointing configured")
        
        # Set parallelism
        env.set_parallelism(config.parallelism)
        logger.info(f"Parallelism set to {config.parallelism}")
        
        # Create data stream from Kafka
        logger.info("Creating Kafka source...")
        ds = env.from_source(
            create_kafka_source(),
            WatermarkStrategy.no_watermarks(),
            "Kafka Clickstream Source"
        )
        logger.info("Kafka source created successfully")
        
        # Apply fraud detection
        logger.info("Applying fraud detection map...")
        processed_ds = ds.map(
            FraudDetectionMapFunction(),
            output_type=Types.STRING()
        ).name("Fraud Detection")
        logger.info("Fraud detection map added")
        
        # Split stream: all events and fraud alerts
        # Sink 1: All labeled events
        logger.info("Adding sink for all events...")
        processed_ds.sink_to(
            create_kafka_sink(config.output_topic)
        ).name("Output to clickstream_labeled")
        logger.info(f"Sink added to {config.output_topic}")
        
        # Sink 2: Only fraud alerts (filter)
        logger.info("Adding sink for fraud alerts...")
        processed_ds.filter(
            lambda x: json.loads(x).get('fraud_prediction', False)
        ).sink_to(
            create_kafka_sink(config.alert_topic)
        ).name("Fraud Alerts")
        logger.info(f"Fraud alerts sink added to {config.alert_topic}")
        
        # Execute job with timeout
        logger.info("Submitting Flink job to cluster...")
        logger.info(f"This may take 30-60 seconds...")
        
        # Set a timeout for execution
        import threading
        def execute_job():
            try:
                env.execute("Real-time Fraud Detection with ML")
            except Exception as e:
                logger.error(f"Job execution failed: {e}", exc_info=True)
        
        thread = threading.Thread(target=execute_job, daemon=False)
        thread.start()
        thread.join(timeout=90)  # Wait max 90 seconds
        
        if thread.is_alive():
            logger.warning("Job submission timed out (90s), but job may still be running on cluster")
        else:
            logger.info("Job submitted and completed")
        
    except Exception as e:
        logger.error(f"Failed to execute Flink job: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()