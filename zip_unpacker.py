import zipfile
from pathlib import Path

# Define paths
zip_folder = "Weather_zipped" 
output_folder = "Weather"

# Ensure the output directory exists
Path(output_folder).mkdir(parents=True, exist_ok=True)

# Loop through each ZIP file in the folder
for zip_file in Path(zip_folder).glob("*.zip"):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file in zip_ref.namelist():
            # Exclude _MACOSX and its contents (some macOS garbage downloaded, stripping it from folders)
            if file.startswith("_MACOSX/") or file.endswith('/'):
                continue
            
            # Check if the file is inside a "weather/" folder
            if file.startswith("weather/"):
                # Reconstruct the relative path inside the output folder
                relative_path = Path(file).relative_to("weather")
                target_path = Path(output_folder) / relative_path
                
                # Ensure the target directory exists
                target_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Extract the file
                with zip_ref.open(file) as source, open(target_path, "wb") as target_file:
                    target_file.write(source.read())

print(f"All 'weather' folders and their year subfolders have been merged into: {output_folder}, excluding '_MACOSX'.")