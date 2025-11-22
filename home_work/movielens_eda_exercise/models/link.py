from sqlalchemy import Column, Integer

from .support import DatasetConfiguration
from movielens_eda_exercise.database import Base


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Link(Base):
    """SQLAlchemy ORM model for links table."""
    __tablename__ = "links"
    id = Column(Integer, primary_key=True, autoincrement=True)
    movie_id = Column(Integer, nullable=True)
    imdb_id = Column(Integer, nullable=True)
    tmdb_id = Column(Integer, nullable=True)


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================

LinksConfiguration = DatasetConfiguration(name="links", inner_path="ml-latest-small/links.csv",
                                          columns=["movieId", "imdbId", "tmdbId"])
