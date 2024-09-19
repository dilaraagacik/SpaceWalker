from sqlmodel import SQLModel, Field, Session, create_engine, select
from sqlalchemy.exc import IntegrityError
from hashlib import md5
from typing import Optional, List, Tuple


DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker" 


engine = create_engine(DATABASE_URL)

class Protein(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    hash: str = Field(max_length=128, nullable=False, unique=True)
    sequence: str = Field(nullable=False)

def read_fasta(file_path: str) -> List[Tuple[str, str]]:
    sequences = []
    with open(file_path, 'r') as file:
        sequence_id = ""
        sequence = ""
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

def get_md5(string: str) -> str:
    md5_hash = md5()
    md5_hash.update(string.encode())
    return md5_hash.hexdigest()

def upload_sequences_to_db(file_path: str):
    sequences = read_fasta(file_path)
    existing_hashes = []
    with Session(engine) as session:
        for sequence_id, sequence in sequences:
            hash_value = get_md5(sequence)
            existing_protein = session.exec(select(Protein).where(Protein.hash == hash_value)).first() #check if protein exists
            if existing_protein:
                existing_hashes.append((hash_value, sequence_id))
            else:
                protein = Protein(hash=hash_value, sequence=sequence)
                session.add(protein)
                session.commit()
    
    for hash_value, sequence_id in existing_hashes:
        print(f"Hash: {hash_value}, Identifier: {sequence_id}")


fasta_file_path = "lipases.fasta" 
upload_sequences_to_db(fasta_file_path)