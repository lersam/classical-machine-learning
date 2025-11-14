from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, Float, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# ============================================================================
# Pydantic Models for MovieLens Datasets
# ============================================================================


class GenomeScoreModel(BaseModel):
    """Contains movie-tag relevance data (0-1 scale)."""
    movie_id: int = Field(..., alias="movieId")
    tag_id: int = Field(..., alias="tagId")
    relevance: float

    class Config:
        populate_by_name = True


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class GenomeScore(Base):
    """SQLAlchemy ORM model for genome scores table."""
    __tablename__ = "genome_scores"

    movie_id = Column(Integer, primary_key=True)
    tag_id = Column(Integer, primary_key=True)
    relevance = Column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("movie_id", "tag_id"),
    )


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================

genome_scores = {  # contains movie-tag relevance data
    "inner_path": "ml-latest/genome-scores.csv",
    "columns": ["movieId", "tagId", "relevance"]
}
