from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint

from .support import DatasetConfiguration
from ..database import Base


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class Tag(Base):
    """SQLAlchemy ORM model for tags table."""
    __tablename__ = "tags"

    user_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, primary_key=True)
    tag = Column(String(255), nullable=False)
    timestamp = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "movie_id", "tag"),
    )


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================
TagConfiguration = DatasetConfiguration(name="tag", inner_path="ml-latest/tags.csv",
                                        columns=["userId", "movieId", "tag", "timestamp"])
