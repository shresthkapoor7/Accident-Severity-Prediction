"""
Traffic Accident Severity Prediction using Apache Spark MLlib
=============================================================
Predicts the Severity (1-4) of traffic accidents based on real-time factors.

Big Data Challenge: Processing 7.7 million records using Apache Spark
ML Challenge: Handling heavily imbalanced data using class weights

IMPROVEMENTS FOR BETTER ACCURACY (Inspired by Kaggle best practices):
- Using GBT (Gradient Boosting Trees) instead of Random Forest (XGBoost-style)
- Enhanced feature engineering (TimeOfDay, Season, interactions)
- Optimized hyperparameters (150 iterations, maxDepth=10, stepSize=0.05)
- Improved class weight calculation for imbalanced data
- Better feature selection and interaction terms
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when, lit, create_map, count
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from itertools import chain
import time
import pandas as pd
import numpy as np
import argparse

def create_spark_session():
    """Initialize Spark Session with optimized configurations for large datasets."""
    import os
    
    # Set Java options to avoid security manager issues
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'
    
    try:
        # Stop any existing Spark session (common issue in Colab/Jupyter)
        try:
            existing_spark = SparkSession.getActiveSession()
            if existing_spark is not None:
                print("WARNING: Stopping existing Spark session...")
                existing_spark.stop()
                # Give it a moment to fully stop
                import time
                time.sleep(1)
        except:
            pass  # No existing session or error stopping it
        
        # Try creating Spark session with full configuration
        try:
            spark = SparkSession.builder \
                .appName("AccidentSeverityPrediction") \
                .config("spark.driver.memory", "12g") \
                .config("spark.executor.memory", "12g") \
                .config("spark.driver.maxResultSize", "6g") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.default.parallelism", "100") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200") \
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200") \
                .master("local[*]") \
                .getOrCreate()
        except Exception as e1:
            print(f"WARNING: Failed with full config, trying simpler configuration...")
            # Fallback: Try with minimal configuration (common in Colab)
            try:
                spark = SparkSession.builder \
                    .appName("AccidentSeverityPrediction") \
                    .config("spark.driver.memory", "4g") \
                    .config("spark.executor.memory", "4g") \
                    .master("local[*]") \
                    .getOrCreate()
                print("Created Spark session with minimal configuration")
            except Exception as e2:
                # Last resort: Try with absolute minimum
                print(f"WARNING: Failed with minimal config, trying absolute minimum...")
                spark = SparkSession.builder \
                    .appName("AccidentSeverityPrediction") \
                    .master("local[*]") \
                    .getOrCreate()
                print("Created Spark session with minimum configuration")
        
        # Validate that Spark session was created successfully
        if spark is None:
            raise RuntimeError("Failed to create Spark session - got None")
        
        # Validate sparkContext exists
        if not hasattr(spark, 'sparkContext') or spark.sparkContext is None:
            raise RuntimeError("Spark session created but sparkContext is None")
        
        # Set log level - use INFO to see stage progress, WARN to reduce verbosity
        # INFO shows stage progress bars which are helpful for monitoring training
        # In Colab, you'll see progress like: [Stage 95:==============================> (56 + 8) / 100]
        spark.sparkContext.setLogLevel("INFO")
        
        return spark
    
    except Exception as e:
        print(f"\nERROR: Error creating Spark session: {e}")
        raise

def load_data(spark, filepath, sample_fraction=None):
    """Load CSV data into Spark DataFrame with schema inference."""
    print("=" * 60)
    print("STEP 1: DATA INGESTION")
    print("=" * 60)
    
    if not filepath:
        raise ValueError("CSV file path is required!")
    
    import os
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    
    # Validate file extension
    file_ext = os.path.splitext(filepath)[1].lower()
    if file_ext not in ['.csv', '']:
        print(f"\nWARNING: File extension is '{file_ext}', expected '.csv'")
        print("   The file might not be a CSV file. Continuing anyway...")
    
    # Check if file looks like a Jupyter kernel file (common mistake)
    if 'kernel-' in filepath and filepath.endswith('.json'):
        raise ValueError(
            f"\nERROR: The file path appears to be a Jupyter kernel file, not a CSV file!\n"
            f"   File: {filepath}\n"
            f"   This looks like a Jupyter runtime file, not your data file.\n"
            f"   Please provide the path to your CSV file (e.g., 'US_Accidents_March23.csv')\n"
            f"   Example usage: python accident_severity_prediction.py US_Accidents_March23.csv"
        )
    
    start_time = time.time()
    
    print(f"Loading data from CSV file: {filepath}")
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    
    record_count = df.count()
    column_count = len(df.columns)
    column_names = df.columns
    
    print(f"Loaded {record_count:,} records with {column_count} columns")
    
    # Validate that this looks like a real CSV file
    if column_count == 1 and column_names[0] in ['{', '_c0', 'value']:
        raise ValueError(
            f"\nERROR: The file doesn't appear to be a valid CSV file!\n"
            f"   File: {filepath}\n"
            f"   Found only 1 column: {column_names[0]}\n"
            f"   This might be a JSON file or incorrectly formatted CSV.\n"
            f"   Please check:\n"
            f"   1. The file is actually a CSV file\n"
            f"   2. The file has headers in the first row\n"
            f"   3. You're providing the correct file path\n"
            f"   Example: python accident_severity_prediction.py US_Accidents_March23.csv"
        )
    
    # Show first few column names for verification
    print(f"\nColumn names (first 10): {column_names[:10]}")
    if column_count > 10:
        print(f"   ... and {column_count - 10} more columns")
    
    # Check if Severity column exists (case-insensitive)
    severity_found = any(col.lower() == "severity" for col in column_names)
    if not severity_found:
        print("\nWARNING: No 'Severity' column found (case-insensitive check)")
        print("   The script will attempt to continue, but may fail if Severity column is missing.")
    
    # Apply sampling if requested
    if sample_fraction is not None and 0 < sample_fraction < 1:
        print(f"\nSampling {sample_fraction*100:.1f}% of data for faster training...")
        df = df.sample(fraction=sample_fraction, seed=42)
        sampled_count = df.count()
        print(f"Sampled dataset: {sampled_count:,} records ({sample_fraction*100:.1f}% of original)")
    
    print(f"Load time: {time.time() - start_time:.2f} seconds")
    
    return df

def find_column_case_insensitive(df, column_name):
    """Find a column in DataFrame case-insensitively."""
    all_columns = [field.name for field in df.schema.fields]
    for col_name in all_columns:
        if col_name.lower() == column_name.lower():
            return col_name
    return None

def explore_data(df):
    """Perform initial data exploration."""
    print("\n" + "=" * 60)
    print("STEP 2: DATA EXPLORATION")
    print("=" * 60)
    
    # Show schema
    print("\nDataset Schema (first 20 columns):")
    for field in df.schema.fields[:20]:
        print(f"   - {field.name}: {field.dataType}")
    
    # Show all columns if there are more than 20
    if len(df.schema.fields) > 20:
        print(f"\n   ... and {len(df.schema.fields) - 20} more columns")
    
    # Check if Severity column exists (case-insensitive)
    all_columns = [field.name for field in df.schema.fields]
    severity_col = find_column_case_insensitive(df, "Severity")
    
    if severity_col is None:
        print("\nWARNING: 'Severity' column not found in the dataset!")
        print("\nAvailable columns in the dataset:")
        for i, col_name in enumerate(all_columns, 1):
            print(f"   {i}. {col_name}")
        print("\nERROR: The dataset must contain a 'Severity' column (case-insensitive).")
        print("   Please check your CSV file and ensure it has a 'Severity' column.")
        raise ValueError(f"'Severity' column not found. Available columns: {all_columns[:10]}...")
    
    # Show severity distribution
    print(f"\nSeverity Distribution (Target Variable) - using column '{severity_col}':")
    severity_dist = df.groupBy(severity_col).count().orderBy(severity_col)
    severity_dist.show()
    
    # Calculate class imbalance
    total = df.count()
    severity_counts = severity_dist.collect()
    print("Class Imbalance Analysis:")
    for row in severity_counts:
        pct = (row['count'] / total) * 100
        print(f"   Severity {row[severity_col]}: {row['count']:,} records ({pct:.2f}%)")
    
    return df

def clean_data(df):
    """Clean and preprocess the data - ETL Pipeline."""
    print("\n" + "=" * 60)
    print("STEP 3: DATA CLEANING (ETL)")
    print("=" * 60)
    
    initial_count = df.count()
    
    # Find columns case-insensitively
    column_mapping = {}
    desired_columns = [
        "Severity", "Start_Lat", "Start_Lng", "Temperature(F)", 
        "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)",
        "Weather_Condition", "Sunrise_Sunset", "Start_Time",
        "Crossing", "Junction", "Traffic_Signal"
    ]
    
    for desired_col in desired_columns:
        found_col = find_column_case_insensitive(df, desired_col)
        if found_col:
            column_mapping[desired_col] = found_col
    
    # Check for critical columns
    if "Severity" not in column_mapping:
        all_columns = [field.name for field in df.schema.fields]
        print(f"\nERROR: 'Severity' column not found!")
        print(f"Available columns: {all_columns}")
        raise ValueError("'Severity' column is required but not found in the dataset")
    
    # Select columns using their actual names (case-insensitive match)
    existing_columns = list(column_mapping.values())
    df_clean = df.select(existing_columns)
    
    # Rename columns to standard names for easier processing
    for desired_col, actual_col in column_mapping.items():
        if actual_col != desired_col:
            df_clean = df_clean.withColumnRenamed(actual_col, desired_col)
    
    print(f"Selected {len(existing_columns)} relevant columns")
    
    # Drop rows with null values in critical columns
    critical_columns = ["Severity", "Start_Lat", "Start_Lng", "Start_Time"]
    critical_columns = [c for c in critical_columns if c in df_clean.columns]
    df_clean = df_clean.dropna(subset=critical_columns)
    
    after_critical_drop = df_clean.count()
    print(f"Dropped {initial_count - after_critical_drop:,} rows with missing critical values")
    
    # Filter out invalid severity values (keep only 1-4)
    df_clean = df_clean.filter((col("Severity") >= 1) & (col("Severity") <= 4))
    
    # Convert severity to 0-based indexing (0, 1, 2, 3) for better ML compatibility
    # Original: 1, 2, 3, 4 -> New: 0, 1, 2, 3
    df_clean = df_clean.withColumn("Severity", col("Severity") - 1.0)
    
    # Cast severity to double for ML
    df_clean = df_clean.withColumn("Severity", col("Severity").cast(DoubleType()))
    
    # Handle missing values in numerical columns with median imputation
    numerical_cols = ["Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)"]
    numerical_cols = [c for c in numerical_cols if c in df_clean.columns]
    
    if numerical_cols:
        # Fill nulls with column means for simplicity (Imputer requires no nulls in output)
        for col_name in numerical_cols:
            mean_val = df_clean.select(col_name).agg({col_name: "mean"}).collect()[0][0]
            if mean_val is not None:
                df_clean = df_clean.fillna({col_name: mean_val})
    
    # Handle missing categorical values
    if "Weather_Condition" in df_clean.columns:
        df_clean = df_clean.fillna({"Weather_Condition": "Unknown"})
    if "Sunrise_Sunset" in df_clean.columns:
        df_clean = df_clean.fillna({"Sunrise_Sunset": "Day"})
    
    final_count = df_clean.count()
    print(f"Final clean dataset: {final_count:,} records")
    print(f"Data retention rate: {(final_count/initial_count)*100:.2f}%")
    
    return df_clean

def engineer_features(df):
    """Extract and engineer features from the cleaned data - Enhanced version."""
    print("\n" + "=" * 60)
    print("STEP 4: FEATURE ENGINEERING (ENHANCED)")
    print("=" * 60)
    
    # Extract temporal features from Start_Time
    df = df.withColumn("Hour", hour(col("Start_Time")))
    df = df.withColumn("DayOfWeek", dayofweek(col("Start_Time")))
    df = df.withColumn("Month", col("Start_Time").substr(6, 2).cast("int"))
    
    print("Extracted Hour, DayOfWeek, and Month from timestamp")
    
    # Create rush hour indicator (7-9 AM and 4-7 PM)
    df = df.withColumn("IsRushHour", 
        when(((col("Hour") >= 7) & (col("Hour") <= 9)) | 
             ((col("Hour") >= 16) & (col("Hour") <= 19)), 1.0).otherwise(0.0))
    
    print("Created IsRushHour feature")
    
    # Create weekend indicator
    df = df.withColumn("IsWeekend",
        when((col("DayOfWeek") == 1) | (col("DayOfWeek") == 7), 1.0).otherwise(0.0))
    
    print("Created IsWeekend feature")
    
    # Create time of day categories (Morning, Afternoon, Evening, Night)
    df = df.withColumn("TimeOfDay",
        when((col("Hour") >= 6) & (col("Hour") < 12), 1.0)  # Morning
        .when((col("Hour") >= 12) & (col("Hour") < 17), 2.0)  # Afternoon
        .when((col("Hour") >= 17) & (col("Hour") < 22), 3.0)  # Evening
        .otherwise(4.0))  # Night
    
    print("Created TimeOfDay feature")
    
    # Create season feature (if Month available)
    if "Month" in df.columns:
        df = df.withColumn("Season",
            when((col("Month") >= 3) & (col("Month") <= 5), 1.0)  # Spring
            .when((col("Month") >= 6) & (col("Month") <= 8), 2.0)  # Summer
            .when((col("Month") >= 9) & (col("Month") <= 11), 3.0)  # Fall
            .otherwise(4.0))  # Winter
        print("Created Season feature")
    
    # Convert boolean columns to numeric
    boolean_cols = ["Crossing", "Junction", "Traffic_Signal"]
    for col_name in boolean_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name,
                when(col(col_name) == True, 1.0)
                .when(col(col_name) == False, 0.0)
                .otherwise(0.0))
    
    print("Converted boolean features to numeric")
    
    # Create interaction features (important for XGBoost-style models)
    if "Temperature(F)" in df.columns and "Humidity(%)" in df.columns:
        df = df.withColumn("Temp_Humidity_Interaction", 
            col("Temperature(F)") * col("Humidity(%)") / 100.0)
        print("Created Temp_Humidity_Interaction feature")
    
    if "Wind_Speed(mph)" in df.columns and "Visibility(mi)" in df.columns:
        df = df.withColumn("Wind_Visibility_Interaction",
            col("Wind_Speed(mph)") / (col("Visibility(mi)") + 0.1))  # Avoid division by zero
        print("Created Wind_Visibility_Interaction feature")
    
    # Drop the original Start_Time column (no longer needed)
    df = df.drop("Start_Time")
    
    # Show sample of engineered features
    print("\nSample of engineered features:")
    sample_cols = ["Hour", "DayOfWeek", "IsRushHour", "IsWeekend", "TimeOfDay"]
    if "Season" in df.columns:
        sample_cols.append("Season")
    df.select(sample_cols).show(5)
    
    return df

def calculate_class_weights(df):
    """Calculate class weights to handle imbalanced data - Improved method."""
    print("\n" + "=" * 60)
    print("STEP 5: HANDLING CLASS IMBALANCE (IMPROVED)")
    print("=" * 60)
    
    # Calculate class distribution
    class_counts = df.groupBy("Severity").count().collect()
    
    total_samples = sum([row["count"] for row in class_counts])
    num_classes = len(class_counts)
    
    # Improved weight calculation: using sklearn's balanced method
    # weight = n_samples / (n_classes * np.bincount(y))
    # This gives more balanced weights
    class_weights = {}
    print("\nClass Weights (Balanced - Improved):")
    print("Note: Severity is 0-based (0, 1, 2, 3) corresponding to original (1, 2, 3, 4)")
    for row in class_counts:
        severity = row["Severity"]
        count = row["count"]
        # Balanced weight formula (similar to sklearn)
        weight = total_samples / (num_classes * count)
        class_weights[severity] = weight
        pct = (count / total_samples) * 100
        original_severity = int(severity) + 1  # Convert back to 1-4 for display
        print(f"   Severity {original_severity} (index {int(severity)}): {count:>12,} samples ({pct:>5.2f}%) -> weight = {weight:.4f}")
    
    # Create mapping expression for adding weights
    mapping_expr = create_map([lit(x) for x in chain(*class_weights.items())])
    
    # Add weight column to dataset
    df_weighted = df.withColumn("classWeight", mapping_expr[col("Severity")])
    
    print("\nAdded classWeight column to handle imbalance")
    print("  Using balanced class weights for better minority class performance")
    
    return df_weighted

def build_ml_pipeline(df):
    """Build the ML pipeline with feature transformers and classifier."""
    print("\n" + "=" * 60)
    print("STEP 6: BUILDING ML PIPELINE")
    print("=" * 60)
    
    stages = []
    
    # String Indexer for Weather_Condition
    if "Weather_Condition" in df.columns:
        weather_indexer = StringIndexer(
            inputCol="Weather_Condition", 
            outputCol="Weather_Index",
            handleInvalid="skip"
        )
        weather_encoder = OneHotEncoder(
            inputCols=["Weather_Index"], 
            outputCols=["Weather_Vec"]
        )
        stages.extend([weather_indexer, weather_encoder])
        print("Added Weather_Condition encoder")
    
    # String Indexer for Sunrise_Sunset
    if "Sunrise_Sunset" in df.columns:
        sunrise_indexer = StringIndexer(
            inputCol="Sunrise_Sunset", 
            outputCol="Sunrise_Index",
            handleInvalid="skip"
        )
        sunrise_encoder = OneHotEncoder(
            inputCols=["Sunrise_Index"], 
            outputCols=["Sunrise_Vec"]
        )
        stages.extend([sunrise_indexer, sunrise_encoder])
        print("Added Sunrise_Sunset encoder")
    
    # Define feature columns for the assembler
    numerical_features = [
        "Start_Lat", "Start_Lng", "Hour", "DayOfWeek",
        "IsRushHour", "IsWeekend"
    ]
    
    # Add enhanced features if they exist
    enhanced_features = ["TimeOfDay", "Month", "Season"]
    for col_name in enhanced_features:
        if col_name in df.columns:
            numerical_features.append(col_name)
    
    # Add optional numerical features if they exist
    optional_numerical = ["Temperature(F)", "Humidity(%)", "Pressure(in)", 
                          "Visibility(mi)", "Wind_Speed(mph)",
                          "Crossing", "Junction", "Traffic_Signal",
                          "Temp_Humidity_Interaction", "Wind_Visibility_Interaction"]
    
    for col_name in optional_numerical:
        if col_name in df.columns:
            numerical_features.append(col_name)
    
    # Build final feature list
    feature_cols = numerical_features.copy()
    if "Weather_Condition" in df.columns:
        feature_cols.append("Weather_Vec")
    if "Sunrise_Sunset" in df.columns:
        feature_cols.append("Sunrise_Vec")
    
    print(f"Feature columns: {len(feature_cols)} features")
    
    # Vector Assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    stages.append(assembler)
    print("Added VectorAssembler")
    
    # Build feature preparation pipeline (without classifier)
    # We'll use One-vs-Rest with GBTClassifier, so we don't add the classifier here
    feature_pipeline = Pipeline(stages=stages)
    
    print("Built feature preparation pipeline")
    print("  Will use One-vs-Rest with GBTClassifier for multiclass classification")
    
    return feature_pipeline

def train_and_evaluate_ovr(df, feature_pipeline):
    """Train One-vs-Rest GBTClassifier models and evaluate performance."""
    from pyspark.sql.functions import when, col
    
    print("\n" + "=" * 60)
    print("STEP 7: MODEL TRAINING (One-vs-Rest with GBTClassifier)")
    print("=" * 60)
    
    # Split data: 80% train, 20% test
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    
    train_count = train_data.count()
    test_count = test_data.count()
    
    print(f"Training set: {train_count:,} records")
    print(f"Test set: {test_count:,} records")
    
    # Apply feature pipeline to prepare features
    print("\nPreparing features...")
    feature_model = feature_pipeline.fit(train_data)
    train_features = feature_model.transform(train_data)
    test_features = feature_model.transform(test_data)
    
    # Cache feature dataframes
    train_features.cache()
    test_features.cache()
    
    # Train 4 GBTClassifier models using One-vs-Rest
    models = {}
    severity_levels = [0, 1, 2, 3]  # 0-based severity levels
    
    print("\nTraining 4 GBTClassifier models (One-vs-Rest)...")
    print("   This will take longer as we're training 4 separate models...")
    
    total_start_time = time.time()
    
    for severity in severity_levels:
        print(f"\n   Training model for Severity {severity} (vs. All Others)...")
        
        # Create binary label: 1 if this severity, 0 otherwise
        binary_train = train_features.withColumn(
            "binary_label",
            when(col("Severity") == severity, 1.0).otherwise(0.0)
        )
        
        # Calculate class weights for this binary problem
        pos_count = binary_train.filter(col("binary_label") == 1.0).count()
        neg_count = binary_train.filter(col("binary_label") == 0.0).count()
        total_count = pos_count + neg_count
        
        if pos_count > 0 and neg_count > 0:
            # Calculate balanced weights
            pos_weight = total_count / (2.0 * pos_count)
            neg_weight = total_count / (2.0 * neg_count)
            
            binary_train = binary_train.withColumn(
                "binary_classWeight",
                when(col("binary_label") == 1.0, pos_weight).otherwise(neg_weight)
            )
            
            print(f"      Positive samples: {pos_count:,} (weight: {pos_weight:.4f})")
            print(f"      Negative samples: {neg_count:,} (weight: {neg_weight:.4f})")
        else:
            binary_train = binary_train.withColumn("binary_classWeight", col("classWeight"))
            print(f"      Using original class weights")
        
        # Train GBTClassifier for this binary problem
        gbt = GBTClassifier(
            labelCol="binary_label",
            featuresCol="features",
            weightCol="binary_classWeight",
            maxIter=100,  # Number of boosting iterations
            maxDepth=12,  # Maximum depth of trees
            stepSize=0.1,  # Learning rate
            minInstancesPerNode=10,
            seed=42,
            maxBins=32
        )
        
        start_time = time.time()
        model = gbt.fit(binary_train)
        training_time = time.time() - start_time
        
        models[severity] = model
        print(f"      Model {severity} trained in {training_time:.2f} seconds ({training_time/60:.2f} minutes)")
    
    total_training_time = time.time() - total_start_time
    print(f"\nAll 4 models trained in {total_training_time:.2f} seconds ({total_training_time/60:.2f} minutes)")
    
    # Make predictions using all 4 models
    print("\nMaking predictions using One-vs-Rest ensemble...")
    
    # Add a unique row identifier to test_features for joining
    from pyspark.sql.functions import monotonically_increasing_id
    test_features_with_id = test_features.withColumn("row_id", monotonically_increasing_id())
    
    # Get probability predictions from each model
    prediction_dfs = []
    for severity in severity_levels:
        binary_test = test_features_with_id.withColumn(
            "binary_label",
            when(col("Severity") == severity, 1.0).otherwise(0.0)
        )
        
        # Get probability of positive class (this severity)
        pred = models[severity].transform(binary_test)
        # Extract probability of class 1 (positive)
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        get_prob = udf(lambda v: float(v[1]), DoubleType())
        pred = pred.withColumn(f"prob_severity_{severity}", get_prob("probability"))
        prediction_dfs.append(pred.select("row_id", "Severity", f"prob_severity_{severity}"))
    
    # Combine all probability predictions using row_id
    combined = prediction_dfs[0]
    for i in range(1, len(prediction_dfs)):
        combined = combined.join(prediction_dfs[i], on="row_id", how="inner")
    
    # Find the severity with highest probability
    from pyspark.sql.functions import greatest, when as spark_when
    
    prob_cols = [col(f"prob_severity_{s}") for s in severity_levels]
    max_prob = greatest(*prob_cols)
    
    # Determine prediction based on highest probability
    prediction_expr = spark_when(
        col("prob_severity_0") == max_prob, 0.0
    ).when(
        col("prob_severity_1") == max_prob, 1.0
    ).when(
        col("prob_severity_2") == max_prob, 2.0
    ).otherwise(3.0)
    
    combined = combined.withColumn("prediction", prediction_expr)
    
    # Join back with test_features to get all columns
    predictions = test_features_with_id.join(
        combined.select("row_id", "prediction"),
        on="row_id",
        how="inner"
    ).drop("row_id")
    
    # Unpersist cached data
    train_features.unpersist()
    test_features.unpersist()
    
    # Return models dict, feature_model, predictions, and test_data
    return models, feature_model, predictions, test_data

def evaluate_model(predictions):
    """Evaluate model performance with multiple metrics."""
    print("\n" + "=" * 60)
    print("STEP 8: MODEL EVALUATION")
    print("=" * 60)
    
    # Accuracy
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = accuracy_evaluator.evaluate(predictions)
    
    # Weighted Precision
    precision_evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = precision_evaluator.evaluate(predictions)
    
    # Weighted Recall
    recall_evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = recall_evaluator.evaluate(predictions)
    
    # F1 Score (most important for imbalanced data)
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="f1"
    )
    f1_score = f1_evaluator.evaluate(predictions)
    
    print("\nMODEL PERFORMANCE METRICS:")
    print("-" * 40)
    print(f"   Accuracy:           {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"   Weighted Precision: {precision:.4f} ({precision*100:.2f}%)")
    print(f"   Weighted Recall:    {recall:.4f} ({recall*100:.2f}%)")
    print(f"   F1 Score:           {f1_score:.4f} ({f1_score*100:.2f}%)")
    print("-" * 40)
    
    # Per-class metrics
    print("\nConfusion Matrix Analysis:")
    predictions.groupBy("Severity", "prediction").count().orderBy("Severity", "prediction").show(20)
    
    # Prediction distribution
    print("\nPrediction Distribution:")
    predictions.groupBy("prediction").count().orderBy("prediction").show()
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score
    }

def extract_feature_importance(models_dict, feature_names):
    """Extract and display feature importance from all trained models (averaged)."""
    print("\n" + "=" * 60)
    print("STEP 9: FEATURE IMPORTANCE ANALYSIS")
    print("=" * 60)
    
    try:
        # Collect importances from all models
        all_importances = []
        for severity, model in models_dict.items():
            if hasattr(model, 'featureImportances'):
                importances = model.featureImportances.toArray()
                all_importances.append(importances)
            else:
                print(f"WARNING: Model for Severity {severity} doesn't support feature importance extraction")
        
        if not all_importances:
            print("WARNING: No models support feature importance extraction")
            return None
        
        # Average importances across all models
        avg_importances = np.mean(all_importances, axis=0)
        
        # Create feature importance pairs
        feature_importance = list(zip(feature_names, avg_importances))
        
        # Sort by importance (descending)
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        print("\nTop 20 Most Important Features (Averaged across all 4 models):")
        print("-" * 60)
        for i, (feature, importance) in enumerate(feature_importance[:20], 1):
            bar = "█" * int(importance * 100)
            print(f"{i:2}. {feature:30} {importance:.4f} {bar}")
        
        return feature_importance
    
    except Exception as e:
        print(f"WARNING: Could not extract feature importance: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_model(models_dict, feature_model, base_path="accident_severity_model"):
    """Save all trained models (feature pipeline + 4 GBTClassifier models) to disk."""
    print("\n" + "=" * 60)
    print("STEP 10: SAVING MODELS")
    print("=" * 60)
    
    try:
        # Save feature preparation pipeline
        feature_path = f"{base_path}_features"
        feature_model.write().overwrite().save(feature_path)
        print(f"Feature pipeline saved to: {feature_path}")
        
        # Save each GBTClassifier model
        for severity, model in models_dict.items():
            model_path = f"{base_path}_severity_{severity}"
            model.write().overwrite().save(model_path)
            print(f"Model for Severity {severity} saved to: {model_path}")
        
        print(f"\nAll models saved successfully!")
        print(f"   Base path: {base_path}")
    except Exception as e:
        print(f"WARNING: Could not save models: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main execution function."""
    import sys
    import os
    
    # Filter out IPython/Colab magic command flags (like -f) and kernel files before parsing
    # These flags are used by %run magic command and shouldn't be passed to the script
    filtered_argv = [sys.argv[0]]  # Keep script name
    for arg in sys.argv[1:]:
        # Skip magic command flags
        if arg in ['-f', '-i', '-e', '-t', '-N', '-n', '-p']:
            continue
        # Skip kernel JSON files (common mistake in Colab)
        if 'kernel-' in arg and arg.endswith('.json'):
            print(f"WARNING: Ignoring kernel file argument: {arg}")
            continue
        # Skip if it looks like a kernel file path
        if '/jupyter/runtime/kernel-' in arg:
            print(f"WARNING: Ignoring kernel file path: {arg}")
            continue
        filtered_argv.append(arg)
    
    original_argv = sys.argv
    sys.argv = filtered_argv
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Traffic Accident Severity Prediction using Apache Spark MLlib')
    parser.add_argument('csv_file', nargs='?', help='Path to the CSV file containing accident data')
    parser.add_argument('--sample', type=float, default=None, 
                       help='Sample fraction (0.0 to 1.0) to use for training. Example: 0.1 for 10%%')
    
    # Use parse_known_args to ignore any remaining unknown arguments (Colab compatibility)
    args, unknown = parser.parse_known_args()
    
    # Restore original argv
    sys.argv = original_argv
    
    print("\n" + "=" * 70)
    print("   TRAFFIC ACCIDENT SEVERITY PREDICTION SYSTEM")
    print("   Using Apache Spark MLlib for Big Data Processing")
    print("=" * 70)
    
    # Get CSV file path from command line argument or prompt
    if args.csv_file:
        csv_filepath = args.csv_file
    else:
        # Try to auto-detect CSV file in common Colab locations
        common_csv_names = [
            'US_Accidents_March23.csv',
            'US_Accidents.csv',
            'accidents.csv',
            'data.csv'
        ]
        common_paths = [
            '/content/',
            './',
            ''
        ]
        
        csv_filepath = None
        for path_prefix in common_paths:
            for csv_name in common_csv_names:
                test_path = os.path.join(path_prefix, csv_name) if path_prefix else csv_name
                if os.path.exists(test_path):
                    csv_filepath = test_path
                    print(f"\nAuto-detected CSV file: {csv_filepath}")
                    break
            if csv_filepath:
                break
        
        # If still not found, prompt user
        if not csv_filepath:
            print("\n📁 Please provide the path to your CSV file")
            print("   Example: US_Accidents_March23.csv")
            print("   Or: /content/US_Accidents_March23.csv")
            csv_filepath = input("Enter CSV file path: ").strip()
            
            # Remove quotes if user added them
            csv_filepath = csv_filepath.strip('"').strip("'")
            
            if not csv_filepath:
                print("ERROR: No file path provided. Exiting.")
                sys.exit(1)
    
    # Early validation - check if it looks like a wrong file
    if 'kernel-' in csv_filepath and '.json' in csv_filepath:
        print(f"\nERROR: The provided path looks like a Jupyter kernel file, not a CSV!")
        print(f"   Path: {csv_filepath}")
        print(f"\n   This is likely a mistake. Please provide the path to your CSV file.")
        print(f"   Example: US_Accidents_March23.csv")
        print(f"   Or if the file is in your current directory, just use: US_Accidents_March23.csv")
        sys.exit(1)
    
    # Validate file exists
    if not os.path.exists(csv_filepath):
        print(f"\nERROR: File not found: {csv_filepath}")
        print(f"\n   Please check:")
        print(f"   1. The file path is correct")
        print(f"   2. The file exists in the specified location")
        print(f"   3. You're in the correct directory")
        print(f"\n   If the CSV file is in your current directory, you can use just the filename.")
        print(f"   Example: US_Accidents_March23.csv")
        sys.exit(1)
    
    # Validate file extension
    if not csv_filepath.lower().endswith('.csv'):
        print(f"\nWARNING: File doesn't have .csv extension: {csv_filepath}")
        print(f"   Continuing anyway, but make sure this is a CSV file...")
    
    # Validate sample fraction if provided
    sample_fraction = args.sample
    if sample_fraction is not None:
        if sample_fraction <= 0 or sample_fraction > 1:
            print(f"ERROR: Sample fraction must be between 0 and 1. Got: {sample_fraction}")
            sys.exit(1)
    
    print(f"\nUsing CSV file: {csv_filepath}")
    if sample_fraction:
        print(f"Sample fraction: {sample_fraction*100:.1f}%")
    
    total_start_time = time.time()
    
    # Step 1: Initialize Spark
    print("\nInitializing Spark Session...")
    spark = None
    try:
        spark = create_spark_session()
        print("Spark Session created successfully")
        print(f"   Spark Version: {spark.version}")
    except Exception as e:
        print(f"\nERROR: Failed to initialize Spark session: {e}")
        sys.exit(1)
    
    try:
        # Step 2: Load Data from CSV (with optional sampling)
        df = load_data(spark, csv_filepath, sample_fraction=sample_fraction)
        
        # Step 3: Explore Data
        df = explore_data(df)
        
        # Step 4: Clean Data (ETL)
        df_clean = clean_data(df)
        
        # Step 5: Feature Engineering
        df_features = engineer_features(df_clean)
        
        # Step 6: Calculate Class Weights
        df_weighted = calculate_class_weights(df_features)
        
        # Cache the dataframe for faster processing
        df_weighted.cache()
        
        # Step 7: Build Feature Preparation Pipeline
        feature_pipeline = build_ml_pipeline(df_weighted)
        
        # Step 8: Train and Evaluate using One-vs-Rest with GBTClassifier
        models_dict, feature_model, predictions, test_data = train_and_evaluate_ovr(df_weighted, feature_pipeline)
        
        # Step 9: Evaluate Model
        metrics = evaluate_model(predictions)
        
        # Step 10: Feature Importance
        # Build feature names list dynamically
        feature_names = [
            "Start_Lat", "Start_Lng", "Hour", "DayOfWeek",
            "IsRushHour", "IsWeekend", "Temperature(F)", "Humidity(%)",
            "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)",
            "Crossing", "Junction", "Traffic_Signal"
        ]
        if "Weather_Condition" in df_features.columns:
            feature_names.append("Weather_Vec")
        if "Sunrise_Sunset" in df_features.columns:
            feature_names.append("Sunrise_Vec")
        
        extract_feature_importance(models_dict, feature_names)
        
        # Step 11: Save Models
        save_model(models_dict, feature_model)
        
        # Summary
        total_time = time.time() - total_start_time
        print("\n" + "=" * 70)
        print("   EXECUTION COMPLETE")
        print("=" * 70)
        print(f"\nTotal execution time: {total_time/60:.2f} minutes")
        print(f"Final F1 Score: {metrics['f1_score']:.4f}")
        print("\nKey Takeaways:")
        print("   - One-vs-Rest approach with GBTClassifier (XGBoost-like)")
        print("   - 4 binary GBTClassifier models trained (one per severity level)")
        print("   - Class weights applied to each binary model")
        print("   - Predictions combined using highest probability")
        print("   - F1 Score is the primary metric for imbalanced classification")
        
    except Exception as e:
        print(f"\nERROR: Error during execution: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark Session (only if it was created successfully)
        if spark is not None:
            try:
                spark.stop()
                print("\nSpark Session stopped")
            except Exception as e:
                print(f"\nWARNING: Error stopping Spark session: {e}")
        else:
            print("\nWARNING: Spark Session was not created, skipping cleanup")

if __name__ == "__main__":
    main()
