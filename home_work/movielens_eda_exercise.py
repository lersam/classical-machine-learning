import requests
import pandas as pd

from typing import Union
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO

from sqlalchemy import create_engine

from models.link import LinksConfiguration
from models.movie import MovieConfiguration
from models.tag import TagConfiguration
from models.rating import Base

engine = create_engine("sqlite:///movielens.db")
Base.metadata.create_all(engine)


def download_movielens_data(zip_path: Path, reload=False) -> Union[Path, None]:
    """Downloads the MovieLens latest dataset zip file to the specified path.
    If the file already exists and reload is False, it will not download again."""

    movielens_url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"

    if zip_path.exists() and not reload:
        return zip_path

    try:
        response = requests.get(movielens_url, stream=True)
        response.raise_for_status()
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        with zip_path.open(mode='wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return zip_path

    except requests.exceptions.RequestException as e:
        print(f"**❌ Error during download:** {e}")
        return None


def load_ml_csvs_from_zip(zip_path: Path, models_struct: list) -> dict:
    """Loads specified CSV files from the MovieLens zip archive into pandas DataFrames."""
    z = ZipFile(BytesIO(zip_path.read_bytes()))
    laded_models = {}
    for single_model in models_struct:
        laded_models[single_model.name] = pd.read_csv(z.open(single_model.inner_path), names=single_model.columns)
    return laded_models


if __name__ == "__main__":
    zip_path = Path(Path(__file__).parent, "local_data/ml-latest.zip")

    movielen_path = download_movielens_data(zip_path=zip_path)
    if movielen_path is None:
        exit(1)

    models = load_ml_csvs_from_zip(zip_path=zip_path,
                                   models_struct=[LinksConfiguration, TagConfiguration, MovieConfiguration])
