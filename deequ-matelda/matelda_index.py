import os

# Function to process and add matelda_index to each line, including the header
def add_index_to_csv(input_path, output_path):
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        # Read the header line
        header = infile.readline().strip()
        # Write the new header with matelda_index added
        outfile.write(f"matelda_index,{header}\n")
        
        # Iterate over the rest of the lines, adding a sequential index to each
        for idx, line in enumerate(infile):
            outfile.write(f"{idx},{line}")

# Root directory containing the directories with clean.csv and dirty.csv
root_dir = '/Users/fatemehahmadi/Documents/matelda-deequ/datasets/REIN'
# Directory where modified files will be saved
output_root_dir = '/Users/fatemehahmadi/Documents/matelda-deequ/datasets/REIN_matelda_idx'

# Iterate over directories within the root directory
for subdir, _, files in os.walk(root_dir):
    for file_name in ['clean.csv', 'dirty.csv']:
        if file_name in files:
            # Construct the input file path
            input_file_path = os.path.join(subdir, file_name)
            
            # Construct the corresponding output directory and file path
            relative_subdir = os.path.relpath(subdir, root_dir)
            output_dir = os.path.join(output_root_dir, relative_subdir)
            os.makedirs(output_dir, exist_ok=True)
            
            output_file_path = os.path.join(output_dir, file_name)
            
            # Add index and save to the new location
            add_index_to_csv(input_file_path, output_file_path)

print("Processing complete.")
