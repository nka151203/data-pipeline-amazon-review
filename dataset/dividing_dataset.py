import gzip
import json
import os
import boto3
from botocore.exceptions import ClientError


input_file = './Grocery_and_Gourmet_Food.jsonl.gz'
output_dir = './Grocery_and_Gourmet_Food'
lines_per_file = 65000

os.makedirs(output_dir, exist_ok=True)

part_number = 1
line_in_part = 0
writer = None

def open_new_writer(part_number):
    filename = f'Grocery_and_Gourmet_Food_part_{part_number:06d}.jsonl.gz'
    output_path = os.path.join(output_dir, filename)
    return gzip.open(output_path, 'wt', encoding='utf-8')

with gzip.open(input_file, 'rt', encoding='utf-8') as f:
    try:
        writer = open_new_writer(part_number)
        for line_num, line in enumerate(f, 1):
            try:
                record = json.loads(line)

                # Ghi bản ghi
                json.dump(record, writer)
                writer.write('\n')
                line_in_part += 1

                # Kiểm tra nếu đạt 65,000 dòng thì mở file mới
                if line_in_part >= lines_per_file:
                    writer.close()
                    part_number += 1
                    line_in_part = 0
                    writer = open_new_writer(part_number)

            except Exception as e:
                print(f"[Fail in line {line_num}]: {e}")

    finally:
        if writer:
            writer.close()

print(f"Split by line count completed in: {output_dir}")
