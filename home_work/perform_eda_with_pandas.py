import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from home_work.database import engine

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger()

# Detect pandas_profiling (or ydata_profiling)
try:
    from pandas_profiling import ProfileReport  # type: ignore
    has_profile = True
except Exception:
    try:
        from ydata_profiling import ProfileReport  # type: ignore
        has_profile = True
    except Exception:
        has_profile = False
        logger.debug("pandas_profiling / ydata_profiling not available; will produce basic summaries instead")


def perform_eda_with(plots_dir: Optional[Path] = None):
    """Generate pandas profiling reports (or basic summaries) for MovieLens tables."""
    if plots_dir is None:
        plots_dir = Path(__file__).parent / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    try:
        with engine.connect() as conn:
            # Try MySQL-style INFORMATION_SCHEMA first, fall back to sqlite_master
            try:
                tbls = pd.read_sql_query(
                    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()",
                    conn,
                )
                tables = set(tbls['TABLE_NAME'].tolist())
            except Exception as info_err:
                logger.debug("information_schema query failed: %s", info_err)
                try:
                    dialect_name = getattr(engine, "dialect", None) and getattr(engine.dialect, "name", None)
                    if dialect_name == "sqlite":
                        tbls = pd.read_sql_query(
                            "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                            conn,
                        )
                        tables = set(tbls['TABLE_NAME'].tolist())
                    else:
                        raise
                except Exception as sqlite_err:
                    logger.exception("Failed to list tables (info schema and sqlite fallbacks failed): %s", sqlite_err)
                    return

            if 'ratings' not in tables or 'movies' not in tables:
                logger.error("Required tables 'ratings' and 'movies' not found in database. Found: %s", tables)
                return

            ratings = pd.read_sql_table('ratings', conn)
            movies = pd.read_sql_table('movies', conn)

    except Exception as e:
        logger.exception("Failed to read tables from DB: %s", e)
        return

    # Normalize rating column
    if 'rating' in ratings.columns:
        ratings['rating'] = pd.to_numeric(ratings['rating'], errors='coerce')

    # Prepare merged dataframe when possible
    merged = None
    if 'movie_id' in ratings.columns and ('movie_id' in movies.columns or 'movieId' in movies.columns):
        if 'movie_id' in movies.columns:
            merge_cols = ['movie_id'] + (['title'] if 'title' in movies.columns else [])
            movies_for_merge = movies[merge_cols]
        else:
            movies_for_merge = movies
        merged = ratings.merge(movies_for_merge, left_on='movie_id',
                               right_on=movies_for_merge.columns[0], how='left')

    # Generate detailed ProfileReport if available
    if has_profile:
        try:
            logger.debug("Generating ProfileReport reports (this may take a while)...")

            rpt = ProfileReport(ratings, title="Ratings Profile", minimal=False)
            p_r = plots_dir / "ratings_profile_pandas_profiling.html"
            rpt.to_file(str(p_r))
            logger.info("Saved ratings ProfileReport: %s", p_r)

            rpt = ProfileReport(movies, title="Movies Profile", minimal=False)
            p_m = plots_dir / "movies_profile_pandas_profiling.html"
            rpt.to_file(str(p_m))
            logger.info("Saved movies ProfileReport: %s", p_m)

            if merged is not None:
                rpt = ProfileReport(merged, title="Merged Profile", minimal=False)
                p_mm = plots_dir / "merged_profile_pandas_profiling.html"
                rpt.to_file(str(p_mm))
                logger.info("Saved merged ProfileReport: %s", p_mm)

        except Exception as e:
            logger.exception("Failed to generate ProfileReport reports: %s", e)
    else:
        # Fallback: save simple descriptive statistics and head samples
        try:
            logger.debug("pandas_profiling not available; saving basic summaries instead")
            (plots_dir / "ratings_describe.csv").write_text(ratings.describe(include='all').to_csv())
            (plots_dir / "ratings_head.csv").write_text(ratings.head(100).to_csv(index=False))
            (plots_dir / "movies_describe.csv").write_text(movies.describe(include='all').to_csv())
            (plots_dir / "movies_head.csv").write_text(movies.head(100).to_csv(index=False))
            if merged is not None:
                (plots_dir / "merged_describe.csv").write_text(merged.describe(include='all').to_csv())
                (plots_dir / "merged_head.csv").write_text(merged.head(100).to_csv(index=False))
            logger.info("Saved basic CSV summaries to %s", plots_dir)
        except Exception as e:
            logger.exception("Failed to save basic summaries: %s", e)

    logger.info("EDA finished. Outputs saved to %s", plots_dir)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Perform EDA using pandas_profiling / basic summaries")
    parser.add_argument("--plots-dir", type=Path, default=None, help="Directory to save reports")
    args = parser.parse_args()
    perform_eda_with(plots_dir=args.plots_dir)
