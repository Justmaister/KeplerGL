import pandas as pd
import os
import geojson
import logging
from typing import List, Dict
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_directory_exists(directory_path: str) -> None:
    """
    Create directory if it doesn't exist.

    Args:
        directory_path: Path to the directory to create
    """
    try:
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory {directory_path} exists or was created successfully")
    except Exception as e:
        logger.error(f"Error creating directory {directory_path}: {str(e)}")
        raise

def load_metro_data(file_path: str) -> pd.DataFrame:
    """
    Load metro data from CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        pd.DataFrame: Loaded metro data
    """
    try:
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Successfully loaded data from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        raise

def create_geojson_features(df: pd.DataFrame, line: str) -> List[geojson.Feature]:
    """
    Create GeoJSON features for a specific metro line.

    Args:
        df: DataFrame containing metro data
        line: Metro line identifier

    Returns:
        List[geojson.Feature]: List of GeoJSON features
    """
    try:
        line_df = df.loc[df['route_short_name'] == line]
        trips_dict = dict(tuple(line_df.groupby('trip_id')))
        features = []

        for trip_id, trip_data in trips_dict.items():
            geometry = [
                (lon, lat, elev, time)
                for lon, lat, elev, time in zip(
                    trip_data.stop_lon,
                    trip_data.stop_lat,
                    trip_data.elevation,
                    trip_data.arrival_time_int
                )
            ]

            feature = geojson.Feature(
                geometry=geojson.LineString(geometry),
                properties=dict(
                    trip=trip_data.trip_id.unique().tolist(),
                    stop=trip_data.stop_id.unique().tolist(),
                    route=trip_data.route_short_name.unique().tolist()
                )
            )
            features.append(feature)

        return features
    except Exception as e:
        logger.error(f"Error creating features for line {line}: {str(e)}")
        raise

def save_geojson_file(features: List[geojson.Feature], output_path: str) -> None:
    """
    Save GeoJSON features to file.

    Args:
        features: List of GeoJSON features
        output_path: Path where to save the file
    """
    try:
        ensure_directory_exists(os.path.dirname(output_path))

        feature_collection = geojson.FeatureCollection(features)

        with open(output_path, 'w', encoding='utf8') as fp:
            geojson.dump(feature_collection, fp, sort_keys=True, ensure_ascii=False)

        logger.info(f"Successfully saved GeoJSON file to {output_path}")
    except Exception as e:
        logger.error(f"Error saving GeoJSON file to {output_path}: {str(e)}")
        raise

def main():
    try:
        current_dir = os.getcwd()
        data_file = os.path.join(current_dir, 'data', 'metro_data.csv')
        output_dir = os.path.join(current_dir, 'geojson')

        ensure_directory_exists(output_dir)

        df = load_metro_data(data_file)

        for line in df["route_short_name"].unique():
            logger.info(f"Processing line {line}")

            features = create_geojson_features(df, line)

            output_file = os.path.join(output_dir, f'{line}.geojson')
            save_geojson_file(features, output_file)

        logger.info("All lines processed successfully")

    except Exception as e:
        logger.error(f"Error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()