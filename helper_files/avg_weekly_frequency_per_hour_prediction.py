import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import joblib
import json
import math

import helper_files.helper as helper

from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import GradientBoostingRegressor

def calculate_and_save_min_max_values(dataset, model_dir):
    """
    Calculates the min/max values for log-transformed features and saves them to a JSON file.
    These values are needed to correctly scale new data for the score calculations.
    """
    score_features = [
        'shops_nearby_count', 
        'employed_total', 
        'bus_commute_total', 
        'avg_weekly_frequency_per_hour', 
        'population_density'
    ]

    df = dataset[score_features].copy()

    for col in score_features:
        df[col] = df[col].apply(lambda x: math.log(1 + x) if pd.notna(x) else x)

    min_max_values = {
        col: {
            'min': df[col].min() if not df[col].empty else 0,
            'max': df[col].max() if not df[col].empty else 0
        }
        for col in score_features
    }

    with open(os.path.join(model_dir, 'min_max_values.json'), 'w') as f:
        json.dump(min_max_values, f, indent=4)
    print(f"Min/Max values for log-transformed features saved to {os.path.join(model_dir, 'min_max_values.json')}")


def run_prediction_model(model_dir=helper.affix_root_path("models")):
    """
    Loads data, preprocesses it, trains a Gradient Boosting Regressor model,
    and then saves the model and all necessary preprocessors to disk.
    """
    dataset_path = helper.affix_root_path("data/stops_enriched_with_clusters.csv")
    dataset = pd.read_csv(dataset_path)

    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    calculate_and_save_min_max_values(dataset, model_dir)

    X_features = ['shops_nearby_count', 'population_density', 'oa21pop', 'employed_total', 'bus_commute_total', 'customer_convenience_score', 'commute_opportunity_score']
    
    X = dataset[X_features]
    y = dataset['avg_weekly_frequency_per_hour']

    imputer_mean_X = SimpleImputer(missing_values=np.nan, strategy='mean')
    X_clean_np = imputer_mean_X.fit_transform(X)
    X_clean = pd.DataFrame(X_clean_np, columns=X_features)

    y_clean = y.fillna(y.mean())
    
    scaler_X = StandardScaler()
    X_scaled_np = scaler_X.fit_transform(X_clean)
    X_scaled = pd.DataFrame(X_scaled_np, columns=X_features)

    scaler_y = MinMaxScaler()
    y_scaled_np = scaler_y.fit_transform(y_clean.values.reshape(-1, 1))
    y_scaled = pd.Series(y_scaled_np.flatten(), name=y_clean.name)

    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y_scaled, test_size=0.2, random_state=0)

    model = GradientBoostingRegressor(random_state=0)
    model.fit(X_train, y_train)
    print("Model training complete.")

    y_pred = model.predict(X_test)
    
    y_test_original = scaler_y.inverse_transform(y_test.values.reshape(-1, 1)).flatten()
    y_pred_original = scaler_y.inverse_transform(y_pred.reshape(-1, 1)).flatten()
    
    joblib.dump(model, os.path.join(model_dir, 'gradient_boosting_model.joblib'))
    joblib.dump(imputer_mean_X, os.path.join(model_dir, 'imputer_X.joblib'))
    joblib.dump(scaler_X, os.path.join(model_dir, 'scaler_X.joblib'))
    joblib.dump(scaler_y, os.path.join(model_dir, 'scaler_y.joblib'))
    print(f"Model and preprocessors saved to {model_dir}")

    plt.figure(figsize=(10, 6))
    plt.scatter(y_test_original, y_pred_original, color='g', alpha=0.6)
    plt.title('Predicted vs. Actual (Gradient Boosting Regressor)', fontsize=14)
    plt.ylabel('Predicted Avg Weekly Services per Hour', fontsize=12)
    plt.xlabel('Actual Avg Weekly Services per Hour', fontsize=12)
    plt.axline((0, 0), slope=1, color='r', linestyle='--')
    plt.ticklabel_format(style='plain')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    plt.savefig(os.path.join(model_dir, 'predicted_vs_actual.png'), dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Plot saved to {model_dir}")


def predict_on_new_data(new_data_row):
    """
    Loads a saved model and its preprocessors to make a prediction
    on a new, single row of data.
    """
    model_dir = helper.affix_root_path("models")

    try:
        loaded_model = joblib.load(os.path.join(model_dir, 'gradient_boosting_model.joblib'))
        loaded_imputer_X = joblib.load(os.path.join(model_dir, 'imputer_X.joblib'))
        loaded_scaler_X = joblib.load(os.path.join(model_dir, 'scaler_X.joblib'))
        loaded_scaler_y = joblib.load(os.path.join(model_dir, 'scaler_y.joblib'))
    except FileNotFoundError:
        return "Error: Saved model or preprocessors not found. Please run run_prediction_model() first."

    new_data_imputed = pd.DataFrame(
        loaded_imputer_X.transform(new_data_row),
        columns=new_data_row.columns
    )
    
    new_data_scaled = loaded_scaler_X.transform(new_data_imputed)

    scaled_prediction = loaded_model.predict(new_data_scaled)
    
    original_prediction = loaded_scaler_y.inverse_transform(scaled_prediction.reshape(-1, 1)).flatten()
    
    return original_prediction[0]

