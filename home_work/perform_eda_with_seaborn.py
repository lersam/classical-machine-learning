import pandas as pd
import seaborn as sns
import logging

import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional
from home_work.database import engine

# Try to import the modern profiling package (ydata-profiling) and fall back
# to the older pandas_profiling namespace if available. If neither is installed
# we'll skip generating profile reports and log a warning.
try:
    from ydata_profiling import ProfileReport  # type: ignore
except Exception:
    try:
        from pandas_profiling import ProfileReport  # type: ignore
    except Exception:
        ProfileReport = None

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger()

def perform_eda_with_seaborn(plots_dir: Optional[Path] = None, min_ratings: int = 50):
    """Performs exploratory data analysis using seaborn on the MovieLens DB.

    Parameters
    - plots_dir: optional Path to save plots. Defaults to a `plots/` folder next to this file.
    - min_ratings: minimum number of ratings for a movie to be considered 'popular'.
    """
    logger.debug("start EDA with seaborn")

    if plots_dir is None:
        plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    try:
        with engine.connect() as conn:
            tbls = pd.read_sql_query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()", conn)
            tables = set(tbls['TABLE_NAME'].tolist())
            if 'ratings' not in tables or 'movies' not in tables:
                logger.error("Required tables 'ratings' and 'movies' not found in database. Found: %s", tables)
                return

            ratings = pd.read_sql_table('ratings', conn)
            movies = pd.read_sql_table('movies', conn)

    except Exception as e:
        logger.exception("Failed to read tables from DB: %s", e)
        return

    # Normalize common column names (camelCase -> snake_case)
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {}
        for c in df.columns:
            if c == 'userId' or c.lower() == 'userid':
                rename_map[c] = 'user_id'
            if c == 'movieId' or c.lower() == 'movieid':
                rename_map[c] = 'movie_id'
            if c == 'imdbId':
                rename_map[c] = 'imdb_id'
            if c == 'tmdbId':
                rename_map[c] = 'tmdb_id'
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    ratings = _normalize(ratings)
    movies = _normalize(movies)

    # Convert rating column to numeric
    if 'rating' in ratings.columns:
        ratings['rating'] = pd.to_numeric(ratings['rating'], errors='coerce')

    # Generate pandas-profiling / ydata-profiling reports if available
    if ProfileReport is not None:
        try:
            logger.debug("Generating ProfileReport for ratings and movies (this may take a while)...")
            # Ratings report (use minimal mode to keep memory usage reasonable)
            ratings_report = ProfileReport(ratings, title="Ratings Profile", minimal=True)
            p_ratings = plots_dir / 'ratings_profile.html'
            ratings_report.to_file(p_ratings)
            logger.info("Saved ratings profile report: %s", p_ratings)

            # Movies report
            movies_report = ProfileReport(movies, title="Movies Profile", minimal=True)
            p_movies = plots_dir / 'movies_profile.html'
            movies_report.to_file(p_movies)
            logger.info("Saved movies profile report: %s", p_movies)

            # Merged report when possible
            if 'movie_id' in ratings.columns and ('movie_id' in movies.columns or 'movieId' in movies.columns):
                # prepare a merged dataframe for profiling (include title when available)
                if 'movie_id' in movies.columns:
                    merge_cols = ['movie_id'] + ([ 'title' ] if 'title' in movies.columns else [])
                    movies_for_merge_prof = movies[merge_cols]
                else:
                    movies_for_merge_prof = movies

                merged_prof = ratings.merge(movies_for_merge_prof, left_on='movie_id', right_on=movies_for_merge_prof.columns[0], how='left')
                merged_report = ProfileReport(merged_prof, title="Merged Ratings+Movies Profile", minimal=True)
                p_merged = plots_dir / 'merged_profile.html'
                merged_report.to_file(p_merged)
                logger.info("Saved merged profile report: %s", p_merged)

        except Exception as e:
            logger.exception("Failed to generate ProfileReport reports: %s", e)
    else:
        logger.warning("ProfileReport package not available. Install 'ydata-profiling' to enable HTML profiling reports.")

    logger.info('EDA finished. Plots saved to %s', plots_dir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Perform EDA on MovieLens data using seaborn")
    parser.add_argument("--plots-dir", type=Path, default=None, help="Directory to save plots")
    parser.add_argument("--min-ratings", type=int, default=50, help="Minimum ratings for a movie to be considered popular (default: 50)")

    args = parser.parse_args()

    perform_eda_with_seaborn(plots_dir=args.plots_dir, min_ratings=args.min_ratings)
