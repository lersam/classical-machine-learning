from sqlalchemy import Column, Integer, String

from . import Base
from .support import DatasetConfiguration


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Movie(Base):
    """SQLAlchemy ORM model for movies table."""
    __tablename__ = "movies"

    movie_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    genres = Column(String(500), nullable=True)


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================
MovieConfiguration = DatasetConfiguration(name="movies", inner_path="ml-latest/movies.csv",
                                          columns=["movieId", "title", "genres"])
