# kmeans_enrichment.py

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import helper_files.logger as logger
import joblib
import json
import helper_files.helper as helper
import os


def kmeans_model(stops_enriched_csv_path = helper.affix_root_path("data/stops_enriched.csv"), 
                 output_filename = helper.affix_root_path("data/stops_enriched_with_clusters.csv"),
                 model_dir = helper.affix_root_path("models")
                 ):
    """
    Creates and saves a kmeans model, and adds two new fields to the csv. (cluster and cluster_category)
    """
    df = pd.read_csv(stops_enriched_csv_path)

    logger.log("DataFrame before imputation:")
    logger.log(df)


    shops_median = df[df['shops_nearby_count'] != -1]['shops_nearby_count'].median()
    logger.log(f"\nCalculated median for 'shops_nearby_count': {shops_median}")
    df['shops_nearby_count'] = df['shops_nearby_count'].replace(-1, shops_median)


    for col in ['oa21pop', 'employed_total']:
        median_value = df[col].median()
        logger.log(f"Calculated median for '{col}': {median_value}")
        df[col] = df[col].fillna(median_value)


    for col in ['postcode', 'oa21cd', 'lsoa21cd', 'lsoa21nm']:
        df[col] = df[col].fillna('Unknown')

    logger.log("\nDataFrame after imputation:")
    logger.log(df)


    features = ['oa21pop', 'shops_nearby_count', 'employed_total']
    X = df[features]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    num_clusters = 3
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)

    df['cluster'] = kmeans.fit_predict(X_scaled)

    try:
        with open(os.path.join(model_dir, "cluster_dict.json"), 'r') as f:
            cluster_mapping = json.load(f)
        cluster_mapping = {int(k): v for k, v in cluster_mapping.items()}
        logger.log("\nLoaded cluster mapping from JSON.")
    except FileNotFoundError:
        logger.log("\nError: 'cluster_mapping.json' not found. Using default mapping.")
        cluster_mapping = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

    df['cluster_category'] = df['cluster'].map(cluster_mapping)
    logger.log("Added 'cluster_category' column to the DataFrame.")

    df.to_csv(output_filename, index=False)

    joblib.dump(kmeans, os.path.join(model_dir, "kmeans_model.joblib"))
    logger.log("K-Means model saved to 'kmeans_model.joblib'")

    joblib.dump(scaler, os.path.join(model_dir, 'kmeans_scaler.joblib'))
    logger.log("StandardScaler saved to 'scaler.joblib'")

    logger.log(f"Clustering complete. Found {num_clusters} clusters.")
    logger.log("\nFirst 5 rows with new cluster labels:")
    logger.log(df[['stop_name', 'oa21pop', 'shops_nearby_count', 'employed_total', 'cluster']].head())

    cluster_summary = df.groupby('cluster')[features].mean()
    logger.log("\nCluster Summary (Mean values for each feature):")
    logger.log(cluster_summary)