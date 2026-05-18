#!/usr/bin/env python3
"""Diagnostic script to check feature mismatch"""

import pickle
import json
import onnxruntime as rt
import numpy as np
import os

def diagnose():
    print("=" * 60)
    print("FLINK FRAUD DETECTION - FEATURE MISMATCH DIAGNOSIS")
    print("=" * 60)
    
    # 1. Check model metadata
    print("\n1. MODEL METADATA (model_metadata.json):")
    with open('/opt/flink/ml_job/model_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    expected_features = metadata['input_schema']['num_features']
    feature_list = metadata['input_schema']['features']
    print(f"   Expected features: {expected_features}")
    print(f"   Feature list length: {len(feature_list)}")
    print(f"   Features: {feature_list}")
    
    # 2. Check feature columns pickle
    print("\n2. FEATURE COLUMNS PICKLE (feature_columns.pkl):")
    try:
        with open('/opt/flink/ml_job/feature_columns.pkl', 'rb') as f:
            columns = pickle.load(f)
        print(f"   Number of columns: {len(columns)}")
        print(f"   Columns: {columns}")
    except Exception as e:
        print(f"   ERROR: {e}")
        columns = []
    
    # 3. Check ONNX model input shape
    print("\n3. ONNX MODEL INPUT SHAPE:")
    try:
        session = rt.InferenceSession('/opt/flink/ml_job/random_forest_clickstream.onnx',
                                      providers=['CPUExecutionProvider'])
        input_name = session.get_inputs()[0].name
        input_shape = session.get_inputs()[0].shape
        print(f"   Input name: {input_name}")
        print(f"   Input shape: {input_shape}")
        
        # Try to infer expected features from shape
        if len(input_shape) == 2:
            print(f"   Expected number of features: {input_shape[1]}")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # 4. Summary
    print("\n4. SUMMARY:")
    print(f"   Metadata expects: {expected_features} features")
    print(f"   Pickle has: {len(columns)} features")
    print(f"   MISMATCH: {expected_features != len(columns)}")
    
    # 5. Recommendations
    if expected_features != len(columns):
        print("\n5. RECOMMENDATIONS:")
        if len(columns) > expected_features:
            print(f"   ⚠️  Pickle has {len(columns) - expected_features} extra features!")
            print(f"   Option A: Update model_metadata.json to reflect {len(columns)} features")
            print(f"   Option B: Retrain model with {expected_features} features from metadata")
        else:
            print(f"   ⚠️  Pickle has {expected_features - len(columns)} fewer features!")
            print(f"   Option A: Update model_metadata.json to reflect {len(columns)} features")
            print(f"   Option B: Retrain model with {len(columns)} features")

if __name__ == '__main__':
    diagnose()
