#!/usr/bin/env python
from qdrant_client import QdrantClient, models
import hashlib
import numpy as np
import h5py
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Qdrant client
client = QdrantClient(url="http://localhost:6333")

# Function to calculate MD5 hash from a sequence
def calculate_md5(sequence: str) -> str:
    md5_hash = hashlib.md5(sequence.encode()).hexdigest()
    return md5_hash

# Function to read sequences from a FASTA file
def read_fasta(file_path: str) -> dict:
    sequences = {}
    with open(file_path, 'r') as file:
        sequence = ''
        header = ''
        for line in file:
            if line.startswith('>'):
                if sequence:
                    uniprot_id = header.split('|')[1]
                    sequences[uniprot_id] = sequence
                    sequence = ''
                header = line.strip()
            else:
                sequence += line.strip()
        if sequence:
            uniprot_id = header.split('|')[1]
            sequences[uniprot_id] = sequence
    return sequences

# Function to read vectors from an HDF5 file
def read_vectors_from_hdf5(file_path: str) -> dict:
    vectors = {}
    with h5py.File(file_path, 'r') as f:
        for dataset_name in f:
            uniprot_id = dataset_name  # Use the dataset name directly as the UniProt ID
            raw_point = f[dataset_name][:]
            vector = np.array(raw_point)
            vectors[uniprot_id] = vector
    return vectors

# Function to create the proteins collection in Qdrant
def create_proteins_collection():
    try:
        client.create_collection(
            collection_name="proteins",
            vectors_config=models.VectorParams(size=1024, distance=models.Distance.COSINE),
        )
        print("Collection 'proteins' created successfully.")
    except Exception as e:
        if "already exists" in str(e):
            print("Collection 'proteins' already exists.")
        else:
            print(f"Failed to create collection: {e}")

# Main function to process and upload data to Qdrant
def upload_to_qdrant(fasta_file: str, hdf5_file: str):
    # Create the proteins collection
    create_proteins_collection()

    # Read sequences and vectors
    sequences = read_fasta(fasta_file)
    vectors = read_vectors_from_hdf5(hdf5_file)

    # Iterate over sequences and vectors
    for uniprot_id, sequence in sequences.items():
        if uniprot_id in vectors:
            vector = vectors[uniprot_id]
            hash_value = calculate_md5(sequence)
            logger.info(f"Calculated MD5 hash for sequence: {hash_value}")
            point = models.PointStruct(
                id=hash_value,
                payload={
                    "protein ID": uniprot_id,
                    "sequence": sequence,
                    "hash": hash_value
                },
                vector=vector.tolist(),
            )
            try:
                logger.info(f"Attempting to insert point with ID {hash_value} for UniProt ID {uniprot_id}")
                client.upsert(
                    collection_name="proteins",
                    points=[point],
                )
                logger.info(f"Inserted point with ID {hash_value} for UniProt ID {uniprot_id}")
            except Exception as e:
                logger.error(f"Failed to insert point with ID {hash_value} for UniProt ID {uniprot_id}: {e}")
                continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload sequences and vectors to Qdrant.')
    parser.add_argument('fasta_file', type=str, help='Path to the FASTA file')
    parser.add_argument('hdf5_file', type=str, help='Path to the HDF5 file')
    args = parser.parse_args()

    upload_to_qdrant(args.fasta_file, args.hdf5_file)