
#This is for turning the cleaned 100k job title list you just did into a python list to be used in the master code

import pandas as pd

def read_csv_to_list_of_strings(file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path, header=None)
        
        # Combine all columns in each row into a single string
        job_descriptions = df.apply(lambda row: ' '.join(row.astype(str)), axis=1).tolist()
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        job_descriptions = []
    
    return job_descriptions

def save_list_to_file(data, output_file):
    try:
        with open(output_file, 'w') as f:
            f.write("job_descriptions = [\n")
            for item in data:
                f.write(f"    '{item}',\n")
            f.write("]\n")
    except Exception as e:
        print(f"Error writing to the output file: {e}")

# Replace 'your_large_file.csv' with the path to your actual CSV file
file_path = 'Cleaned_100k_JobTitle_List.csv'
job_descriptions = read_csv_to_list_of_strings(file_path)

# Output the job_descriptions list to a separate Python file
output_file = 'cleanedJobDescription.py'
save_list_to_file(job_descriptions, output_file)

