from sqlalchemy import Column, Integer
from sqlalchemy.orm import declarative_base

from .support import DatasetConfiguration

Base = declarative_base()


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Link(Base):
    """SQLAlchemy ORM model for links table."""
    __tablename__ = "links"

    movie_id = Column(Integer, primary_key=True)
    imdb_id = Column(Integer, nullable=True)
    tmdb_id = Column(Integer, nullable=True)


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================

LinksConfiguration = DatasetConfiguration(name="links", inner_path="ml-latest/links.csv",
                                          columns=["movieId", "imdbId", "tmdbId"])
