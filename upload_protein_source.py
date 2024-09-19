from sqlmodel import SQLModel, Field, Session, create_engine, select
from typing import Optional, List, Tuple
import logging


class Protein(SQLModel, table=True):
    __tablename__ = "protein"
    id: Optional[int] = Field(default=None, primary_key=True)
    hash: str = Field(max_length=128, nullable=False, unique=True)
    sequence: str = Field(nullable=False)

class Source(SQLModel, table=True):
    __tablename__ = "source"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=128, nullable=False)

class ProteinSource(SQLModel, table=True):
    __tablename__ = "protein_source"
    id: Optional[int] = Field(default=None, primary_key=True)
    f_protein_id: int = Field(foreign_key="protein.id")
    f_source_id: int = Field(foreign_key="source.id", nullable=False)
    identifier: str = Field(nullable=False)

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
    statement = select(Source).where(Source.name == "uniprot")
    uniprot_source = session.exec(statement).first()
    
    if not uniprot_source:
        raise ValueError("Source 'UniProt' not found in the database.")
    
    fasta_data = read_fasta(file_path)
    fasta_dict = {sequence: identifier for identifier, sequence in fasta_data}

    # Retrieve all proteins from the database
    statement = select(Protein)
    proteins = session.exec(statement).all()
    
    for protein in proteins:
        sequence = protein.sequence
        if sequence in fasta_dict:
            identifier = fasta_dict[sequence].split(':', 1)[1].split('AccNumber')[0].strip()
            identifier = identifier[:256]  # Truncate identifier to 256 characters
            
            # Insert into protein_source table
            protein_source = ProteinSource(
                f_protein_id=protein.id,
                f_source_id=uniprot_source.id,
                identifier=identifier
            )
            session.add(protein_source)
    
    session.commit()

DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker"
engine = create_engine(DATABASE_URL)

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    process_fasta_and_insert("lipases.fasta", session)
