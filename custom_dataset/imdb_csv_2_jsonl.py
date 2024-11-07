import pandas as pd
import json

file_path = 'D:\study\SJSU\SEM_1\Data_Warehouse-data_226\Dock\sjsu-data226-main\week11\custom_dataset\imdb_top_1000.csv'
df = pd.read_csv(file_path)


processed_data = []
for index, row in df.iterrows():
    text = f"{row['Genre']} {row['Overview']}"
    processed_data.append({
        "put": f"id:hybrid-search:doc::{index}",
        "fields": {
            "doc_id": index,
            "title": row['Series_Title'],
            "text": text
        }
    })


with open("imdb_data.jsonl", "w") as outfile:
    for entry in processed_data:
        json.dump(entry, outfile)
        outfile.write("\n")
