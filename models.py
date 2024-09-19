#!/usr/bin/env python
from sqlmodel import SQLModel, Field, Session, create_engine, select
from sqlalchemy.exc import IntegrityError
from hashlib import md5
from typing import Optional, List, Tuple
import json
from sqlalchemy.types import JSON
from sqlalchemy import Column
import logging


class Protein(SQLModel, table=True):
    __tablename__ = "protein"
    id: Optional[int] = Field(default=None, primary_key=True)
    hash: str = Field(max_length=128, nullable=False, unique=True)
    sequence: str = Field(nullable=False)

class ProteinSource(SQLModel, table=True):
    __tablename__ = "protein_source"
    id: Optional[int] = Field(default=None, primary_key=True)
    f_source_id: int = Field(foreign_key="source.id")   
    f_protein_id: int = Field(foreign_key="protein.id")
    identifier: str = Field(max_length=128, nullable=False)

class Annotation(SQLModel, table=True):
    __tablename__ = "annotation"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=128, nullable=False)

class ProteinAnnotation(SQLModel, table=True):
    __tablename__ = "protein_annotation"
    id: Optional[int] = Field(default=None, primary_key=True)
    f_protein_id: int = Field(foreign_key="protein.id")
    f_annotation_id: int = Field(foreign_key="annotation.id")
    value: dict = Field(sa_column=Column(JSON))

class Source(SQLModel, table=True):
    __tablename__ = "source"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=128, nullable=False)