from sqlalchemy import Column, Integer, Float, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base

from .support import DatasetConfiguration

Base = declarative_base()


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Rating(Base):
    """SQLAlchemy ORM model for ratings table."""
    __tablename__ = "ratings"

    user_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, primary_key=True)
    rating = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "movie_id"),
    )


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================
RatingsConfiguration = DatasetConfiguration(name="ratings", inner_path="ml-latest/ratings.csv",
                                            columns=['user_id', 'movie_id', 'rating', 'timestamp'])
