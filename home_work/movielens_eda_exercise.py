import requests
import logging

import pandas as pd

from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from home_work.database import engine, Base
from home_work.models.link import LinksConfiguration
from home_work.models.movie import MovieConfiguration
from home_work.models.rating import RatingsConfiguration
from home_work.models.tag import TagConfiguration
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger()

Base.metadata.create_all(engine)


def download_movielens_data(zip_path: Path, reload=False) -> bool:
    """Downloads the MovieLens latest dataset zip file to the specified path.
    If the file already exists and reload is False, it will not download again."""

    # movielens_url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"
    movielens_url = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

    logger.debug("start download: %s", movielens_url)
    if zip_path.exists() and not reload:
        logger.debug("file exist: %s", zip_path)
        return True

    try:
        response = requests.get(movielens_url, stream=True)
        response.raise_for_status()
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        with zip_path.open(mode='wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.debug("success download to: %s", zip_path)
        return True

    except requests.exceptions.RequestException as e:
        logger.exception(f"**❌ Error during download:** {e}")
        return False


def load_ml_csvs_from_zip(zip_path: Path, models_struct: list) -> dict:
    """Loads specified CSV files from the MovieLens zip archive into pandas DataFrames."""

    logger.debug("start load csvs from zip: %s", zip_path)

    laded_models = {}
    for single_model in models_struct:
        laded_models[single_model.name] = load_single_csv_from_zip(
            zip_path=zip_path,
            inner_path=single_model.inner_path,
            columns=single_model.columns
        )
        logger.debug("loaded model: %s", single_model.name)

    logger.debug("finished loading all models")
    return laded_models

def load_single_csv_from_zip(zip_path: Path, inner_path: str, columns: list) -> pd.DataFrame:
    """Loads a single CSV file from the MovieLens zip archive into a pandas DataFrame."""

    logger.debug("start load single csv from zip: %s", zip_path)
    z = ZipFile(BytesIO(zip_path.read_bytes()))

    df = pd.read_csv(z.open(inner_path), names=columns, low_memory=False)
    logger.debug("loaded single csv: %s", inner_path)

    return df

def saving_to_database(m_name: str, m_date: pd.DataFrame):
    """Saves the loaded MovieLens DataFrames into the database."""
    logger.debug("start saving models to database")
    m_date.to_sql(m_name, engine, if_exists='replace')
    logger.debug("saved model to database: %s", m_name)

def process_model(model_cls):
    name = model_cls.name
    try:
        logger.debug("start processing model: %s", name)
        df = load_single_csv_from_zip(zip_path=zip_path,
                                        inner_path=model_cls.inner_path,
                                        columns=model_cls.columns)
        saving_to_database(name, df)
        logger.debug("finished processing model: %s", name)
    except Exception as e:
        logger.exception("error processing model %s: %s", name, e)
        raise

if __name__ == "__main__":
    zip_path = Path(Path(__file__).parent, "local_data/ml-latest-small.zip")

    load_status = download_movielens_data(zip_path=zip_path)

    if not load_status:
        exit(1)

    # process each model in its own thread: load single csv from zip then save to db
    models = [
        # MovieConfiguration,
        RatingsConfiguration
        # LinksConfiguration,
        # TagConfiguration
        ]

    max_workers = min(4, max(1, len(models)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {executor.submit(process_model, m): m.name for m in models}
        for fut in as_completed(future_to_name):
            name = future_to_name[fut]
            try:
                fut.result()
            except Exception as exc:
                logger.exception("thread failed for %s: %s", name, exc)
