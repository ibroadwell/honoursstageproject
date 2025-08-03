import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import logger
import joblib
import json

df = pd.read_csv('data/stops_enriched.csv')

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
    with open("models/cluster_dict.json", 'r') as f:
        cluster_mapping = json.load(f)
    cluster_mapping = {int(k): v for k, v in cluster_mapping.items()}
    logger.log("\nLoaded cluster mapping from JSON.")
except FileNotFoundError:
    logger.log("\nError: 'cluster_mapping.json' not found. Using default mapping.")
    cluster_mapping = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

df['cluster_category'] = df['cluster'].map(cluster_mapping)
logger.log("Added 'cluster_category' column to the DataFrame.")

output_filename = "data/stops_enriched_with_clusters.csv"
df.to_csv(output_filename, index=False)

joblib.dump(kmeans, 'models/kmeans_model.joblib')
logger.log("K-Means model saved to 'kmeans_model.joblib'")

joblib.dump(scaler, 'models/kmeans_scaler.joblib')
logger.log("StandardScaler saved to 'scaler.joblib'")

logger.log(f"Clustering complete. Found {num_clusters} clusters.")
logger.log("\nFirst 5 rows with new cluster labels:")
logger.log(df[['stop_name', 'oa21pop', 'shops_nearby_count', 'employed_total', 'cluster']].head())

cluster_summary = df.groupby('cluster')[features].mean()
logger.log("\nCluster Summary (Mean values for each feature):")
logger.log(cluster_summary)


fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')

colors = ['red', 'green', 'blue', 'purple', 'orange', 'cyan']

for i in range(num_clusters):
    cluster_data = df[df['cluster'] == i]
    ax.scatter(cluster_data['oa21pop'],
               cluster_data['shops_nearby_count'],
               cluster_data['employed_total'],
               color=colors[i],
               label=f'Cluster {i}',
               s=50)

ax.set_xlabel('OA21 Population')
ax.set_ylabel('Shops Nearby Count')
ax.set_zlabel('Employed Total')
ax.set_title(f'K-Means Clustering of Bus Stops ({num_clusters} Clusters)')
ax.legend()
plt.show()