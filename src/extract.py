import os
import shutil
import json
import hashlib
import pandas as pd
from datetime import datetime

# Folder paths (in Google Drive, via Colab)
source_folder = "/content/drive/MyDrive/LSA_Oct2026/raw"
output_folder = "/content/drive/MyDrive/LSA_Oct2026/data/raw"
os.makedirs(output_folder, exist_ok=True)

# Copy the two CSV files
shutil.copy(source_folder + "/appointments_regional.csv", output_folder + "/appointments_regional.csv")
shutil.copy(source_folder + "/national_categories.csv", output_folder + "/national_categories.csv")

# Convert the Excel file to CSV
table = pd.read_excel(source_folder + "/actual_duration.csv.xlsx")
table.to_csv(output_folder + "/actual_duration.csv", index=False)

# Build the manifest
files = ["appointments_regional.csv", "national_categories.csv", "actual_duration.csv"]
manifest = []

for name in files:
    path = output_folder + "/" + name

    count = 0
    for line in open(path):
        count = count + 1

    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())

    entry = {
        "file_name": name,
        "size_bytes": os.path.getsize(path),
        "sha256": h.hexdigest(),
        "row_count": count - 1,
        "extracted_at": datetime.now().isoformat(),
    }
    manifest.append(entry)

with open(output_folder + "/manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
