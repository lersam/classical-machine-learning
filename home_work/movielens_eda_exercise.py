from typing import Union
import requests
import pandas as pd
from pathlib import Path


# def download_cmovielens_data():
#     # URLs for the MovieLens 100k dataset
#     ratings_url = "https://files.grouplens.org/datasets/movielens/ml-100k/u.data"
#     movies_url = "https://files.grouplens.org/datasets/movielens/ml-100k/u.item"

#     # Column names for the ratings data
#     ratings_columns = ['user_id', 'movie_id', 'rating', 'timestamp']
#     # Column names for the movies data
#     movies_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'IMDb_URL',
#                       'unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy',
#                       'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror',
#                       'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']

#     # Load the datasets into pandas DataFrames
#     ratings = pd.read_csv(ratings_url, sep='\t', names=ratings_columns, encoding='latin-1')
#     movies = pd.read_csv(movies_url, sep='|', names=movies_columns, encoding='latin-1')
    # return ratings, movies

def download_movielens_data(zip_path: Path) -> Union[Path, None]:
        movielens_url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"

        if zip_path.exists():
            return zip_path
        
        try:
            response = requests.get(movielens_url, stream=True)
            response.raise_for_status()
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            with zip_path.open(mode='wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return zip_path

        except requests.exceptions.RequestException as e:
            print(f"**❌ Error during download:** {e}")
            return None

def read_movielens_data(zip_path: Path) -> Union[tuple[pd.DataFrame, pd.DataFrame], None]:
    # Column names for the ratings data
    ratings_columns = ['user_id', 'movie_id', 'rating', 'timestamp']

    # Column names for the movies data
    movies_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'IMDb_URL',
                      'unknown', 'Action', 'Adventure', 'Animation', "Children's", 'Comedy',
                      'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror',
                      'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    ratings = pd.read_csv(zip_path, sep='\t', names=ratings_columns, encoding='latin-1')
    movies = pd.read_csv(zip_path, sep='|', names=movies_columns, encoding='latin-1')
    return ratings, movies

if __name__ == "__main__":
    zip_path = Path(Path(__file__).parent, "local_data/ml-latest.zip")

    movielen_path = download_movielens_data(zip_path=zip_path)
    if movielen_path is None:
        exit(1)

    ratings, movies = read_movielens_data(zip_path=zip_path)
    print(movies.head())  # Print first few rows of movies
    print(ratings.head())  # Print first few rows of ratings