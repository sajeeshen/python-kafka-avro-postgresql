from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, JSON, Numeric, String, text, MetaData
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine


engine = create_engine(
    "postgresql://postgres:admin123@localhost/postgres"
)

Base = declarative_base()
metadata = Base.metadata


class Incident_Report(Base):
    __tablename__ = 'incident_report'

    id = Column(Integer, primary_key=True, nullable=False)
    incident_number = Column(String)
    age = Column(Integer)
    last_name = Column(String)
    address = Column(String)


class User2(Base):
    __tablename__ = 'user2'

    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String)
    age = Column(Integer)
    last_name = Column(String)
    address = Column(String)


# metadata = MetaData(engine)
# metadata.create_all()