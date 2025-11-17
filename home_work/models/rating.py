from sqlalchemy import Column, Integer, Float, PrimaryKeyConstraint

from .support import DatasetConfiguration
from ..database import Base


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Rating(Base):
    """SQLAlchemy ORM model for ratings table."""
    __tablename__ = "ratings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True)
    movie_id = Column(Integer, nullable=True)
    rating = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False)

# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================
RatingsConfiguration = DatasetConfiguration(name="ratings", inner_path="ml-latest-small/ratings.csv",
                                            columns=['userId', 'movieId', 'rating', 'timestamp'])
