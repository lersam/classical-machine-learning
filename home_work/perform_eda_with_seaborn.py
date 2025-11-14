import pandas as pd
import seaborn as sns
import logging

import matplotlib.pyplot as plt
from pathlib import Path
from typing import Optional
from database import engine

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
            tbls = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
            tables = set(tbls['name'].tolist())
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

    # Basic stats
    logger.info("Ratings shape: %s", ratings.shape)
    logger.info("Movies shape: %s", movies.shape)
    if 'rating' in ratings.columns:
        logger.info("Ratings describe: %s", ratings['rating'].describe())

    # Plot 1: rating value counts
    plt.figure(figsize=(8, 4))
    sns.countplot(x='rating', data=ratings)
    plt.title('Rating Counts')
    plt.tight_layout()
    p1 = plots_dir / 'rating_counts.png'
    plt.savefig(p1)
    plt.close()
    logger.debug('Saved plot: %s', p1)

    # Plot 2: rating distribution histogram
    plt.figure(figsize=(8, 4))
    sns.histplot(ratings['rating'], bins=5, kde=False)
    plt.title('Rating Distribution')
    plt.tight_layout()
    p2 = plots_dir / 'rating_distribution.png'
    plt.savefig(p2)
    plt.close()
    logger.debug('Saved plot: %s', p2)

    # Compute per-movie stats (count and mean)
    movie_stats = ratings.groupby('movie_id').agg(count=('rating', 'size'), mean=('rating', 'mean')).reset_index()

    # Merge with movie titles when available
    if 'movie_id' in movies.columns:
        movies_for_merge = movies[['movie_id', 'title']] if 'title' in movies.columns else movies[['movie_id']]
    elif 'movieId' in movies.columns:
        movies_for_merge = movies[['movieId', 'title']].rename(columns={'movieId': 'movie_id'})
    else:
        movies_for_merge = None

    if movies_for_merge is not None:
        movie_stats = movie_stats.merge(movies_for_merge, on='movie_id', how='left')

    # Filter popular movies
    popular = movie_stats[movie_stats['count'] >= min_ratings]

    # Plot 3: top 20 movies by average rating (with min counts)
    top20 = popular.sort_values('mean', ascending=False).head(20)
    if not top20.empty and 'title' in top20.columns:
        plt.figure(figsize=(10, 8))
        sns.barplot(x='mean', y='title', data=top20, palette='viridis')
        plt.title(f'Top 20 Movies by Mean Rating (>={min_ratings} ratings)')
        plt.xlabel('Mean Rating')
        plt.tight_layout()
        p3 = plots_dir / 'top20_mean_rating.png'
        plt.savefig(p3)
        plt.close()
        logger.debug('Saved plot: %s', p3)

    # Plot 4: top 20 most-rated movies
    top_rated = movie_stats.sort_values('count', ascending=False).head(20)
    if not top_rated.empty and 'title' in top_rated.columns:
        plt.figure(figsize=(10, 8))
        sns.barplot(x='count', y='title', data=top_rated, palette='magma')
        plt.title('Top 20 Most-Rated Movies')
        plt.xlabel('Number of Ratings')
        plt.tight_layout()
        p4 = plots_dir / 'top20_most_rated.png'
        plt.savefig(p4)
        plt.close()
        logger.debug('Saved plot: %s', p4)

    logger.info('EDA finished. Plots saved to %s', plots_dir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Perform EDA on MovieLens data using seaborn")
    parser.add_argument("--plots-dir", type=Path, default=None, help="Directory to save plots")
    parser.add_argument("--min-ratings", type=int, default=50, help="Minimum ratings for a movie to be considered popular (default: 50)")

    args = parser.parse_args()

    perform_eda_with_seaborn(plots_dir=args.plots_dir, min_ratings=args.min_ratings)
