from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import ForeignKey
from src.config.database import Base
class RunSample(Base):
 __tablename__='run_samples'
 run_id:Mapped[int]=mapped_column(ForeignKey('diversity_runs.id'),primary_key=True)
 transect_id:Mapped[int]=mapped_column(primary_key=True)
 replicate_index:Mapped[int]=mapped_column(primary_key=True)
 shannon:Mapped[float]=mapped_column()
 simpson:Mapped[float]=mapped_column()
 rarefied:Mapped[float]=mapped_column()
