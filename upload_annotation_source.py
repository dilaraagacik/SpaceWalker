#!/usr/bin/env python
from sqlmodel import SQLModel, Field, Session, create_engine, select
from models import Protein, ProteinSource, Annotation, ProteinAnnotation, Source
from typing import Optional, List, Tuple
import json
from sqlalchemy.types import JSON
from sqlalchemy import Column
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_json(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from file {file_path}: {e}")
        return None

def add_annotations_from_json(session: Session, annotation_data: dict, annotation_name: str):
    try:
        # Retrieve annotation id for the given annotation name
        annotation = session.exec(select(Annotation).where(Annotation.name == annotation_name)).first()
        if not annotation:
            logger.error(f"Annotation '{annotation_name}' not found in the database.")
            return
        annotation_id = annotation.id
        logger.info(f"Retrieved annotation ID for '{annotation_name}': {annotation_id}")

        for identifier, properties in annotation_data.items():
            # Retrieve the corresponding protein source
            protein_source = session.exec(select(ProteinSource).where(ProteinSource.identifier == identifier)).first()
            if not protein_source:
                logger.error(f"Protein source with identifier '{identifier}' not found in the database.")
                continue

            logger.info(f"Processing protein source with identifier: {identifier}")

            # Check if the annotation already exists in the database
            existing_annotation = session.exec(
                select(ProteinAnnotation).where(
                    ProteinAnnotation.f_protein_id == protein_source.f_protein_id,
                    ProteinAnnotation.f_annotation_id == annotation_id
                )
            ).first()

            if existing_annotation:
                logger.info(f"Annotation already exists for protein ID {protein_source.f_protein_id}. Skipping.")
            else:
                # Add the entire properties dictionary as a JSON value
                annotation_source = ProteinAnnotation(
                    f_protein_id=protein_source.f_protein_id,
                    f_annotation_id=annotation_id,
                    value=properties  # Store the entire properties dictionary
                )
                session.add(annotation_source)
                logger.info(f"Added annotation source for protein ID {protein_source.f_protein_id} with properties.")

        session.commit()
        logger.info("Committed all changes to the database.")
    except Exception as e:
        logger.error(f"Error adding annotations from JSON: {e}")

# Example usage
DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker"
engine = create_engine(DATABASE_URL)
SQLModel.metadata.create_all(engine)

# Load the JSON data from the uniprot_annot.json file
annotation_data = load_json("uni.json")

if annotation_data:
    with Session(engine) as session:
        add_annotations_from_json(session, annotation_data, "properties")
else:
    logger.error("Failed to load annotation data from JSON file.")