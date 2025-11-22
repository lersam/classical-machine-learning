from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# ============================================================================
# Pydantic Models for MovieLens Datasets
# ============================================================================


class GenomeTagModel(BaseModel):
    """Provides the tag descriptions for tag IDs in the genome file."""
    tag_id: int = Field(..., alias="tagId")
    tag: str

    class Config:
        populate_by_name = True


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================

class GenomeTag(Base):
    """SQLAlchemy ORM model for genome tags table."""
    __tablename__ = "genome_tags"

    tag_id = Column(Integer, primary_key=True)
    tag = Column(String(255), nullable=False)


# ============================================================================
# Dataset Configuration Dictionaries
# ============================================================================

genome_tags = {  # provides the tag descriptions for the tag IDs in the genome file
    "inner_path": "ml-latest/genome-tags.csv",
    "columns": ["tagId", "tag"]
}
