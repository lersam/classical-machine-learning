from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint

from .support import DatasetConfiguration
from ..database import Base


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Tag(Base):
    """SQLAlchemy ORM model for tags table."""
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    movie_id = Column(Integer, nullable=False)
    tag = Column(String(255), nullable=False)
    timestamp = Column(Integer, nullable=False)


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================
TagConfiguration = DatasetConfiguration(name="tag", inner_path="ml-latest-small/tags.csv",
                                        columns=["userId", "movieId", "tag", "timestamp"])
