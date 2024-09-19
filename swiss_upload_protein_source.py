#!/usr/bin/env python
from sqlmodel import SQLModel, Field, Session, create_engine, select
from models import Protein, ProteinSource, Annotation, ProteinAnnotation, Source
from typing import Optional, List, Tuple
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_fasta(file_path: str) -> List[Tuple[str, str]]:
    sequences = []
    sequence_id = ""
    sequence = ""
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith(">"):
                if sequence:
                    sequences.append((sequence_id, sequence))
                sequence = ""
                sequence_id = line[1:].strip() 
            else:
                sequence += line.strip()
        if sequence:
            sequences.append((sequence_id, sequence))
    return sequences

def process_fasta_and_insert(file_path: str, session: Session):
    # Query for the 'uniprot' source
    statement = select(Source).where(Source.name == "uniprot")
    uniprot_source = session.exec(statement).first()
    
    if not uniprot_source:
        raise ValueError("Source 'UniProt' not found in the database.")
    
    # Query for the 'unknown' source
    statement = select(Source).where(Source.name == "unknown")
    unknown_source = session.exec(statement).first()
    
    if not unknown_source:
        raise ValueError("Source 'unknown' not found in the database.")
    
    fasta_data = read_fasta(file_path)
    fasta_dict = {sequence: identifier for identifier, sequence in fasta_data}

    # Retrieve all proteins from the database
    statement = select(Protein)
    proteins = session.exec(statement).all()
    
    for protein in proteins:
        sequence = protein.sequence
        if sequence in fasta_dict:
            identifier = fasta_dict[sequence]
            
            # Extract the accession number part (e.g., Q6GZX4 from sp|Q6GZX4|001R_FRG3G)
            try:
                parts = identifier.split('|')
                if len(parts) > 2:
                    acc_number = parts[1]
                    logger.info(f"Accession number: {acc_number}")
                else:
                    logger.error(f"Failed to parse accession number from identifier: {identifier}")
                    continue
            except IndexError:
                logger.error(f"Failed to parse accession number from identifier: {identifier}")
                continue

            if acc_number:
                source_id = uniprot_source.id
            else:
                source_id = unknown_source.id
                
            # Insert into protein_source table
            protein_source = ProteinSource(
                f_protein_id=protein.id,
                f_source_id=source_id,
                identifier=acc_number
            )
            session.add(protein_source)
    
    session.commit()

DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker"
engine = create_engine(DATABASE_URL)

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    process_fasta_and_insert("uniprot_sprot.fasta", session)