import pandas as pd
from datetime import datetime
import os
from typing import List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_to_csv(df: pd.DataFrame,
                filename: str,
                output_dir: str,
                index: bool = False,
                encoding: str = 'utf-8') -> None:
    """
    Save DataFrame to CSV file with error handling.

    Args:
        df: DataFrame to save
        filename: Name of the output file
        output_dir: Directory to save the file (default: 'output')
        index: Whether to include the index in the CSV (default: False)
        encoding: File encoding (default: 'utf-8')
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

        file_path = os.path.join(output_dir, filename)

        df.to_csv(file_path, index=index, encoding=encoding)

        logger.info(f"Successfully saved DataFrame to {file_path}")

    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {str(e)}")
        raise

def load_data(
        file_path: str,
        drop_columns: List[str] = None) -> pd.DataFrame:
    """
    Load and preprocess CSV data with error handling.

    Args:
        file_path: Path to the CSV file
        drop_columns: List of columns to drop

    Returns:
        pd.DataFrame: Processed dataframe
    """
    try:
        df = pd.read_csv(file_path, low_memory=False)
        if drop_columns:
            df = df.drop(columns=drop_columns)
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        raise

def process_stop_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process stop times data by converting time columns and handling missing values.
    """
    try:
        df = df.copy()
        time_columns = ["arrival_time", "departure_time"]
        time_format = '%H:%M:%S'
        for col in time_columns:
            df[col] = pd.to_datetime(
                df[col],
                format=time_format,
                errors='coerce')
        return df.dropna()
    except Exception as e:
        logger.error(f"Error processing stop times: {str(e)}")
        raise

def main():
    try:
        current_dir = os.getcwd()
        data_dir = os.path.join(current_dir, 'data')

        trips_columns = ["service_id", "trip_headsign", "direction_id",
                        "shape_id", "wheelchair_accessible"]
        routes_columns = ["route_text_color", "route_url"]
        stops_columns = ["stop_code", "stop_name", "stop_url",
                        "location_type", "parent_station", "wheelchair_boarding"]

        trips = load_data(os.path.join(data_dir, 'trips.txt'), trips_columns)
        routes = load_data(os.path.join(data_dir, 'routes.txt'), routes_columns)
        stops = load_data(os.path.join(data_dir, 'stops.txt'), stops_columns)
        stop_times = load_data(os.path.join(data_dir, 'stop_times.txt'))

        stop_times = process_stop_times(stop_times)

        tripsandroutes = pd.merge(trips, routes, on="route_id", how="inner")
        final_stops = stop_times.merge(stops, on="stop_id", how="inner")

        final_stops['stop_lat'] = pd.to_numeric(final_stops['stop_lat'])
        final_stops['stop_lon'] = pd.to_numeric(final_stops['stop_lon'])

        final = pd.merge(final_stops, tripsandroutes, on="trip_id", how="inner")

        metro = final[final["route_type"] == 1].copy()
        metro["arrival_time_int"] = (metro['arrival_time'] - datetime(1970,1,1)).dt.total_seconds().astype(int)
        metro["elevation"] = 0

        save_to_csv(metro, 'metro_data.csv', data_dir)

        logger.info("Data processing completed successfully")
        return metro

    except Exception as e:
        logger.error(f"Error in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    metro_data = main()