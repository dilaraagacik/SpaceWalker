#!/usr/bin/env python

from qdrant_client import QdrantClient, models
import numpy as np
import h5py
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Qdrant client with increased timeout settings
client = QdrantClient(
    url="http://localhost:6333",
    timeout=60.0  # Increase the timeout to 60 seconds
)

# Create a collection in Qdrant
client.create_collection(
    collection_name="Protein Embeddings",
    vectors_config=models.VectorParams(size=1024, distance=models.Distance.COSINE),
)

def print_hdf5_structure(file_path):
    def print_attrs(name, obj):
        if isinstance(obj, h5py.Dataset):
            logger.info(f"Dataset: {name}")
            logger.info(f"  Shape: {obj.shape}")
            logger.info(f"  Size: {obj.size}")
            logger.info(f"  Data type: {obj.dtype}")
        elif isinstance(obj, h5py.Group):
            logger.info(f"Group: {name}")

    # Open the HDF5 file
    with h5py.File(file_path, 'r') as f:
        # Traverse the file and print information
        f.visititems(print_attrs)

# Replace 'your_file.h5' with the path to your HDF5 file
file_path = 'embeddings.h5'
print_hdf5_structure(file_path)

# Read the embeddings from the HDF5 file and prepare points for Qdrant
points = []
with h5py.File(file_path, 'r') as f:
    for dataset_name in f:
        raw_point = f[dataset_name][:]
        point = models.PointStruct(id=dataset_name, vector=raw_point.tolist(), payload={"protein ID": dataset_name})
        points.append(point)

# Insert points into the Qdrant collection in batches
batch_size = 100  # Adjust batch size as needed
for i in range(0, len(points), batch_size):
    batch = points[i:i + batch_size]
    try:
        operation_info = client.upsert(
            collection_name="Protein Embeddings",
            wait=True,
            points=batch
        )
        logger.info(f"Inserted batch {i // batch_size + 1}: {operation_info}")
    except Exception as e:
        logger.error(f"Failed to insert batch {i // batch_size + 1}: {e}")

# Verify insertion and include vectors in the search results
try:
    response = client.search(
        collection_name="protein_embeddings",
        query_vector=np.zeros(1024).tolist(),  # Example query vector
        limit=10,
        with_vectors=True  # Ensure vectors are included in the search results
    )
    for point in response:
        logger.info(f"ID: {point.id}, Payload: {point.payload}, Vector: {point.vector}")
except Exception as e:
    logger.error(f"Search failed: {e}")

