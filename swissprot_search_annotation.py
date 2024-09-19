#!/usr/bin/env python
import hashlib
import argparse
import logging
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, SearchRequest
from sqlmodel import SQLModel, Field, Session, create_engine, select
from models import Protein, ProteinSource, Annotation, ProteinAnnotation, Source
from sqlalchemy.exc import IntegrityError
from sqlalchemy.types import JSON
from sqlalchemy import Column
from typing import Optional, List, Tuple
from qdrant_client import QdrantClient, models
from protembed.encoder import T5Encoder, EsmEncoder
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager
import ssl
import requests
from Bio import SeqIO
import os
import torch
from qdrant_client.http.models import SearchRequest, NamedVector
import umap
import numpy as np
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://someuser:somepwd@localhost:5432/spacewalker"
engine = create_engine(DATABASE_URL)

client = QdrantClient(
    url="http://localhost:6333",
    timeout=60.0  
)

def calculate_md5(sequence):
    return hashlib.md5(sequence.encode()).hexdigest().replace("-", "")  # Ensure no hyphens in the hash

def read_fasta(file_path: str) -> List[str]:
    sequences = []
    with open(file_path, 'r') as file:
        sequence = ''
        for line in file:
            if line.startswith('>'):
                if sequence:
                    sequences.append(sequence)
                    sequence = ''
            else:
                sequence += line.strip()
        if sequence:
            sequences.append(sequence)
    return sequences

def check_md5_in_database(md5_hash: str) -> bool:
    with Session(engine) as session:
        existing_protein = session.exec(select(Protein).where(Protein.hash == md5_hash)).first()
        return existing_protein is not None

def check_md5_in_qdrant(md5_hash):
    md5_hash = md5_hash.replace("-", "")
    logger.debug(f"MD5 hash after removing hyphens: {md5_hash}")
    try:
        response = client.retrieve(
            collection_name="proteins",
            ids=[md5_hash],
            with_vectors=True
        )
        logger.info(f"Response from Qdrant for {md5_hash}: {response}")
        
        if response is not None and len(response) > 0:
            embedding = response[0].vector
            payload_hash = response[0].payload.get("hash", "").replace("-", "")
            
            logger.info(f"Embedding for MD5 hash {md5_hash}: {embedding}")
            logger.info(f"Payload hash for MD5 hash {md5_hash}: {payload_hash}")
            
            if embedding is not None and len(embedding) == 1024 and payload_hash == md5_hash:
                return True, embedding
            else:
                logger.warning(f"Embedding for MD5 hash {md5_hash} is invalid or missing, or payload hash does not match.")
                return False, None
        else:
            logger.warning(f"No response or empty response for MD5 hash {md5_hash}.")
            return False, None
    except Exception as e:
        logger.error(f"Error checking MD5 hash in Qdrant: {e}")
        return False, None

def calculate_embedding(sequence, encoder):
    embeddings = encoder.embed([sequence])
    return embeddings[0]

def perform_nearest_neighbor_search(embedding):
    client = QdrantClient(host="localhost", port=6333)  
    neighbor_md5_hashes = []

    # Convert embedding to numpy array and flatten it
    embedding = np.array(embedding, dtype=np.float32)

    # Aggregate the sequence of vectors into a single vector by averaging
    if len(embedding.shape) == 2:
        embedding = np.mean(embedding, axis=0)

    # Ensure embedding is a 1D tensor of the expected dimension
    if len(embedding) != 1024:
        raise ValueError(f"Embedding dimension error: expected 1024, got {len(embedding)}")
    
    embedding = torch.tensor(embedding, dtype=torch.float32)
    
    search_result = client.search(
        collection_name="proteins",  
        query_vector=embedding.tolist(),
        limit=200,
        search_params=models.SearchParams(hnsw_ef=128, exact=False)
    )
    for result in search_result:
        neighbor_md5_hashes.append(result.payload["hash"].replace("-", ""))

    return neighbor_md5_hashes

def get_sequence_and_annotations(md5_hash):
    with Session(engine) as session:
        protein = session.exec(select(Protein).where(Protein.hash == md5_hash)).first()

        if not protein:
            return None

        annotations = session.exec(select(ProteinAnnotation).where(ProteinAnnotation.f_protein_id == protein.id)).all()
        source = session.exec(select(ProteinSource).where(ProteinSource.f_protein_id == protein.id)).first()

        return {
            "identifier": source.identifier if source else None,
            "sequence": protein.sequence,
            "annotations": [annotation.value for annotation in annotations]
            
        }

def write_homologs_to_json(query_id, homologs, output_dir):
    output_file = os.path.join(output_dir, f"{query_id}_homologs.json")
    with open(output_file, 'w') as json_file:
        json.dump(homologs, json_file, indent=4)
    logger.info(f"Homologs written to {output_file}")

def process_fasta_file(query_file, encoder, output_dir):
    md5_to_sequence = {}  # Dictionary to store sequences by their MD5 hash
    md5_to_fasta_id = {}  # Dictionary to store FASTA IDs by their MD5 hash

    for record in SeqIO.parse(query_file, "fasta"):
        sequence = str(record.seq)
        fasta_id = record.id
        md5_hash = calculate_md5(sequence)
        md5_to_sequence[md5_hash] = sequence  # Store the sequence by its MD5 hash
        md5_to_fasta_id[md5_hash] = fasta_id  # Store the FASTA ID by its MD5 hash
        
        output_file = os.path.join(output_dir, f"{fasta_id}.fasta")
        with open(output_file, 'w') as out_file:
            if check_md5_in_database(md5_hash):
                found, embedding = check_md5_in_qdrant(md5_hash)
                if found:
                    print(f'Embedding found for {fasta_id} (MD5: {md5_hash})')
                else:
                    print(f'No embedding found for {fasta_id} (MD5: {md5_hash})')
            else:
                embedding = calculate_embedding(sequence, encoder)
                print(f'Calculated embedding for {fasta_id} (MD5: {md5_hash})')
                print(f'Calculated embedding shape: {embedding.shape}')

            # Perform nearest neighbor search for the current embedding
            try:
                neighbor_md5_hashes = perform_nearest_neighbor_search(embedding)
            except ValueError as e:
                print(e)
                continue

            homologs = []
            # Limit the number of homolog sequences to 200
            for neighbor_md5_hash in neighbor_md5_hashes[:200]:
                # Ensure neighbor MD5 hash does not contain hyphens
                neighbor_md5_hash = neighbor_md5_hash.replace("-", "")
                logger.debug(f"Neighbor MD5 hash after removing hyphens: {neighbor_md5_hash}")

                # Retrieve the sequence for the homolog enzyme
                homolog_sequence_info = get_sequence_and_annotations(neighbor_md5_hash)
                if homolog_sequence_info:
                    homologs.append(homolog_sequence_info)
                    homolog_sequence = homolog_sequence_info["sequence"]
                    homolog_identifier = homolog_sequence_info["identifier"]
                    homolog_info = f'>{homolog_identifier}\n{homolog_sequence}\n'
                else:
                    homolog_info = f'>{neighbor_md5_hash}\nSequence not found\n'
                
                print(homolog_info)
                out_file.write(homolog_info)
            
            # Write homologs to a single JSON file for the query
            write_homologs_to_json(fasta_id, homologs, output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a FASTA file and get embeddings for sequences.')
    parser.add_argument('query_file', type=str, help='Path to the query FASTA file')
    parser.add_argument('--encoder', type=str, choices=['ProtT5', 'ESM2-3B', 'ESM2-650M', 'ESM2-150M'], default='ProtT5', help='Encoder model to use')
    parser.add_argument('--use_gpu', action='store_true', help='Use GPU if available')
    parser.add_argument('--local_model_path', type=str, required=True, help='Path to the local directory containing the model files')
    parser.add_argument('--output_dir', type=str, required=True, help='Directory to save output FASTA files and JSON annotation files')
    args = parser.parse_args()

    if args.encoder == 'ProtT5':
        encoder = T5Encoder(model_name=args.local_model_path, use_gpu=args.use_gpu)
    elif args.encoder == 'ESM2-3B':
        encoder = EsmEncoder(model_name=args.local_model_path, use_gpu=args.use_gpu)
    elif args.encoder == 'ESM2-650M':
        encoder = EsmEncoder(model_name=args.local_model_path, use_gpu=args.use_gpu)
    elif args.encoder == 'ESM2-150M':
        encoder = EsmEncoder(model_name=args.local_model_path, use_gpu=args.use_gpu)

    os.makedirs(args.output_dir, exist_ok=True)
    process_fasta_file(args.query_file, encoder, args.output_dir)