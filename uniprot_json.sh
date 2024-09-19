#!/bin/bash

# Input FASTA file
FASTA_FILE="uniprot_sprot.fasta"

# Output JSON file
OUTPUT_FILE="uniprot_annot.json"

# Temporary file to store intermediate JSON data
TEMP_FILE="temp_uniprot.json"

# Function to fetch and parse JSON data from UniProt
fetch_and_parse_uniprot_json() {
    local uniprot_id=$1
    local temp_file=$2

    # Fetch JSON data from UniProt API
    curl -s "https://rest.uniprot.org/uniprotkb/${uniprot_id}.json" -o "$temp_file"

    # Check if the fetch was successful
    if [ $? -ne 0 ]; then
        echo '{
      "Annotation": 0
    }'
        return
    fi

    # Extract countByFeatureType from the JSON data
    jq -r '.extraAttributes.countByFeatureType' "$temp_file"
}

# Initialize the output JSON file
echo "{" > "$OUTPUT_FILE"

# Read the identifiers from the FASTA file and fetch JSON data
while read -r line; do
    if [[ $line == ">"* ]]; then
        uniprot_id=$(echo "$line" | cut -d'|' -f2)
        countByFeatureType=$(fetch_and_parse_uniprot_json "$uniprot_id" "$TEMP_FILE")
        if [ -n "$countByFeatureType" ]; then
            echo "\"$uniprot_id\": $countByFeatureType," >> "$OUTPUT_FILE"
        fi
    fi
done < "$FASTA_FILE"

# Finalize the JSON object in the output file
sed -i '$ s/,$//' "$OUTPUT_FILE"
echo "}" >> "$OUTPUT_FILE"

# Clean up temporary file
rm "$TEMP_FILE"

echo "Parsed JSON data saved to $OUTPUT_FILE"