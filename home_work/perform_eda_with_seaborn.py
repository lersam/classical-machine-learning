import numpy as np
import warnings

# Patch for sweetviz/numpy VisibleDeprecationWarning issue
if not hasattr(np, "VisibleDeprecationWarning"):
    warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')

import pandas as pd

import logging
import sweetviz as sv
from pathlib import Path
from typing import Optional
from home_work.database import engine

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger()


def perform_eda_with(plots_dir: Optional[Path] = None):
    """Performs exploratory data analysis on the MovieLens DB.

    Parameters
    - plots_dir: optional Path to save plots. Defaults to a `plots/` folder next to this file.
    """

    if plots_dir is None:
        plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    try:
        with engine.connect() as conn:
            tbls = pd.read_sql_query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",
                                     conn)
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

    # Generate profiling reports using sweetviz if available
    try:
        logger.debug("Generating sweetviz reports for ratings and movies (this may take a while)...")

        # Ratings report
        ratings_report = sv.analyze(ratings)
        p_ratings = plots_dir / 'ratings_profile_sweetviz.html'
        ratings_report.show_html(str(p_ratings), open_browser=False)
        logger.info("Saved ratings sweetviz report: %s", p_ratings)

        # Movies report
        movies_report = sv.analyze(movies)
        p_movies = plots_dir / 'movies_profile_sweetviz.html'
        movies_report.show_html(str(p_movies), open_browser=False)
        logger.info("Saved movies sweetviz report: %s", p_movies)

        # Merged report when possible (include title when available)
        if 'movie_id' in ratings.columns and ('movie_id' in movies.columns or 'movieId' in movies.columns):
            if 'movie_id' in movies.columns:
                merge_cols = ['movie_id'] + (['title'] if 'title' in movies.columns else [])
                movies_for_merge_prof = movies[merge_cols]
            else:
                movies_for_merge_prof = movies

            merged_prof = ratings.merge(movies_for_merge_prof, left_on='movie_id',
                                        right_on=movies_for_merge_prof.columns[0], how='left')
            merged_report = sv.analyze(merged_prof)
            p_merged = plots_dir / 'merged_profile_sweetviz.html'
            merged_report.show_html(str(p_merged), open_browser=False)
            logger.info("Saved merged sweetviz report: %s", p_merged)

    except Exception as e:
        logger.exception("Failed to generate sweetviz reports: %s", e)

    logger.info('EDA finished. Plots saved to %s', plots_dir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Perform EDA on MovieLens data")
    parser.add_argument("--plots-dir", type=Path, default=None, help="Directory to save plots")

    args = parser.parse_args()

    perform_eda_with(plots_dir=args.plots_dir)
