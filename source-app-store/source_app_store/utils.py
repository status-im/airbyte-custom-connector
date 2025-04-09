import requests
import jwt
from datetime import datetime
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import json
import csv
import os
import time
import gzip
import shutil
import tempfile
import logging

logger = logging.getLogger("airbyte")


def create_jwt_token(key_id: str, issuer_id: str, private_key: str) -> str:
    now = int(datetime.now().timestamp())
    
    payload = {
        "iss": issuer_id,
        "exp": now + 1200,  # 20 minutes expiration
        "aud": "appstoreconnect-v1",
        "iat": now,
        "nbf": now
    }
    
    try:
        key = serialization.load_pem_private_key(
            private_key.encode(),
            password=None,
            backend=default_backend()
        )
    except Exception as e:
        print(f"Error loading private key: {e}")
        return None

    # Create the token
    try:
        return jwt.encode(
            payload=payload,
            key=key,
            algorithm="ES256",
            headers={"kid": key_id, "alg": "ES256"}
        )
    except Exception as e:
        print(f"Error creating token: {e}")
        return None

def get_report_instances(report_id: str, config):
    """Get instances for a specific report."""
    token = create_jwt_token(config["key_id"], config["issuer_id"], config["private_key"])
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Make the request
    response = requests.get(
        f"https://api.appstoreconnect.apple.com/v1/analyticsReports/{report_id}/instances",
        headers=headers
    )

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error for report {report_id}: {response.status_code}")
        print(response.text)
        return None

def decompress_gzip_to_csv(gzip_path, csv_path):
    """Decompress a .gz file to a .csv file."""
    try:
        with gzip.open(gzip_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        # Remove the gzip file after successful extraction
        os.remove(gzip_path)
        return True
    except Exception as e:
        print(f"Error decompressing file: {e}")
        return False

def download_report_instance(instance_id: str, report_name: str, processing_date: str, downloaded_files, config):
    """Download a specific report instance and save as .csv instead of .csv.gz."""
    token = create_jwt_token(config["key_id"], config["issuer_id"], config["private_key"])
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(
        f"https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/{instance_id}/segments",
        headers=headers
    )

    if response.status_code == 200:
        data = response.json()
        
        # Get the URL from the response
        if data.get("data") and len(data["data"]) > 0:
            file_url = data["data"][0]["attributes"]["url"]
            
            # Create filename using report name and processing date
            safe_report_name = "".join(c for c in report_name if c.isalnum() or c in (' ', '-', '_')).strip()
            csv_file_name = f"{safe_report_name}_{processing_date}.csv"
            
            # Download the file (which will be gzipped from the API)
            print(f"Downloading file from: {file_url}")
            file_response = requests.get(file_url)
            
            if file_response.status_code == 200:
                # Create temporary files
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.gz', delete=False) as gz_temp_file:
                    gz_file_path = gz_temp_file.name
                    gz_temp_file.write(file_response.content)
                
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as csv_temp_file:
                    csv_file_path = csv_temp_file.name
                
                # Decompress the gzipped file to CSV
                if decompress_gzip_to_csv(gz_file_path, csv_file_path):
                    print(f"File downloaded and decompressed successfully to: {csv_file_path}")
                    downloaded_files.add(csv_file_name)
                    # Clean up the gzipped file
                    try:
                        os.remove(gz_file_path)
                    except Exception as e:
                        print(f"Warning: Could not remove temporary gzip file: {e}")
                    return csv_file_path
                else:
                    print(f"Failed to decompress file to CSV")
                    return None
            else:
                print(f"Error downloading file: {file_response.status_code}")
                print(file_response.text)
        else:
            print("No data found in the response")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
    return None

      
def read_and_filter_report(file_path, expected_date):
    """
    Read a report file, filter by date, and return cleaned records.
    """
    records = []
    total_records = 0
    filtered_records = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read the first line to determine the format
            first_line = f.readline().strip()
            f.seek(0)  # Reset file pointer
            
            # Determine if it's tab-delimited or comma-delimited
            if '\t' in first_line:
                logger.info("Detected tab-delimited format")
                reader = csv.DictReader(f, delimiter='\t')
            else:
                logger.info("Detected comma-delimited format")
                reader = csv.DictReader(f)
            
            # Process each record
            for record in reader:
                total_records += 1
                
                # Check if the record's Date matches the expected date
                record_date = record.get('Date', '').strip()
                
                # Skip if we couldn't parse the date or if the record date doesn't match
                if not expected_date or record_date != expected_date:
                    logger.debug(f"Skipping record with date {record_date} (does not match expected date {expected_date})")
                    continue
                    
                filtered_records += 1
                
                # Clean and normalize the data
                cleaned_record = {}
                for key, value in record.items():
                    # Clean the key
                    clean_key = key.strip()
                    
                    # Clean the value
                    if value is not None:
                        clean_value = value.strip()
                        if clean_value == '':
                            clean_value = None
                    else:
                        clean_value = None
                        
                    cleaned_record[clean_key] = clean_value
                
                # Add metadata
                cleaned_record["_ab_source_file_url"] = file_path
                cleaned_record["processed_date"] = datetime.now().isoformat()
                
                records.append(cleaned_record)
        
        logger.info(f"Processed {total_records} total records, filtered to {filtered_records} records matching expected date {expected_date}")
        return records
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
        return [] 