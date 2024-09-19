from sqlmodel import SQLModel, Field, Session, create_engine, select
import h5py
import numpy as np
from typing import Dict
from typing import Optional, List, Tuple

class Protein(SQLModel, table=True):
    __tablename__ = "protein"
    id: Optional[int] = Field(default=None, primary_key=True)
    hash: str = Field(max_length=128, nullable=False, unique=True)
    sequence: str = Field(nullable=False)

class ProteinSource(SQLModel, table=True):
    __tablename__ = "protein_source"
    id: Optional[int] = Field(default=None, primary_key=True)
    f_protein_id: int = Field(foreign_key="protein.id")
    f_source_id: int = Field(foreign_key="source.id", nullable=False)
    identifier: str = Field(nullable=False)


def read_h5_hashes(h5_file_path: str) -> Dict[str, np.ndarray]:
    embeddings_dict = {}
    try:
        with h5py.File(h5_file_path, "r") as f:
            for key in f.keys():
                embeddings_dict[key] = f[key][()]
        print(f"Total hashes read: {len(embeddings_dict)}")  # Debugging line
    except Exception as e:
        print(f"Error reading H5 file: {e}")
    return embeddings_dict

def record_exists(session: Session, identifier: str) -> bool:
    statement = select(ProteinSource).where(ProteinSource.identifier == identifier)
    result = session.exec(statement).first()
    return result is not None

def process_and_verify(embeddings_file: str, session: Session):
    # Read the embeddings file
    embeddings_dict = read_h5_hashes(embeddings_file)

    # Retrieve all proteins from the database
    statement = select(Protein)
    proteins = session.exec(statement).all()
    
    non_matching_proteins = []
    
    for protein in proteins:
        protein_hash = protein.hash
        
        if protein_hash not in embeddings_dict:
            non_matching_proteins.append((protein.id, protein_hash))
    
    if non_matching_proteins:
        for protein_id, protein_hash in non_matching_proteins:
            print(f"Hash not found in embeddings for protein ID {protein_id}. Hash: {protein_hash}")
    else:
        print("All hashes match.")

def insert_protein_source(session: Session, protein_id: int, source_id: int, identifier: str):
    if not record_exists(session, identifier):
        new_protein_source = ProteinSource(f_protein_id=protein_id, f_source_id=source_id, identifier=identifier)
        session.add(new_protein_source)
        session.commit()
    else:
        print(f"Record with identifier {identifier} already exists. Skipping insertion.")

DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker"
engine = create_engine(DATABASE_URL)

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    process_and_verify("embeddings.h5", session)
