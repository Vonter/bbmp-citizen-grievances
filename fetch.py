#!/usr/bin/env python3

import pandas as pd
import requests
import time

from pathlib import Path
from bs4 import BeautifulSoup

def load_ids(file_path, data_key=None):
    """Load IDs already fetched"""
    try:
        df = pd.read_parquet(file_path)
        ids = set(int(str(cid)) for cid in df[data_key].dropna())
        print(f"Loaded {len(ids)} IDs from {file_path}")
        return ids
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return set()

def load_failed_ids():
    """Load previously failed IDs"""
    try:
        with open('.failed.txt', 'r') as f:
            failed_ids = set(int(line.strip()) for line in f if line.strip())
        print(f"Loaded {len(failed_ids)} failed IDs from .failed.txt")
        return failed_ids
    except FileNotFoundError:
        print("No failed IDs file found")
        return set()

def save_failed_ids(failed_ids):
    """Save failed IDs to the file"""
    with open('.failed.txt', 'w') as f:
        for failed_id in failed_ids:
            f.write(f"{failed_id}\n")

def extract_panel_content(html_content):
    """Extract content from div with class='panel'."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        panel_div = soup.find('div', class_='panel')
        return str(panel_div) if panel_div else None
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return None

def should_skip(complaint_id, existing_ids, failed_ids):
    """Check if complaint ID should be skipped."""
    return complaint_id in existing_ids or complaint_id in failed_ids

def fetch_complaint_details():
    """Fetch complaint details for missing complaint IDs"""
    existing_ids = load_ids("data/citizen-grievances.parquet", "Complaint ID")
    failed_ids = load_failed_ids()
    
    Path("raw").mkdir(exist_ok=True)
    url = "https://www.smartoneblr.com/WssBBMPComplaintRequestDetails.htm"
    consecutive_invalid = 0
    complaint_id = 20000000
    
    form_data = {
        "compno": "^", "assMntNo": "^", "alfaNo": "^", "SbassMntNo": "^",
        "pageNameV": "waterTaxSearch.htm^", "mobnoFlg": "^", "mobNumber": "^",
        "sessionLangCode": "^", "RefNo": "CSCRefNo^", "deptId": "BBMP^",
        "searchBy": "refNoDiv^", "mobNum": ""
    }
    
    while complaint_id < 21000000:
        if should_skip(complaint_id, existing_ids, failed_ids):
            consecutive_invalid = 0
            complaint_id += 1
            continue
        
        try:
            # Update form data for current ID
            form_data.update({
                "complainantNo": f"{complaint_id}^",
                "applicationNo": f"{complaint_id}^"
            })
            params = {"_show": "Show", "complainantNo": complaint_id}
            
            response = requests.post(url, params=params, data=form_data, timeout=10)
            response.raise_for_status()
            
            if "Grievance Status" in response.text:
                panel_content = extract_panel_content(response.text)
                if panel_content:
                    Path(f"raw/{complaint_id}.html").write_text(panel_content, encoding='utf-8')
                    print(f"Saved: {complaint_id}")
                    consecutive_invalid = 0
                    save_failed_ids(failed_ids)
                else:
                    print(f"No panel: {complaint_id}")
                    consecutive_invalid += 1
                    failed_ids.add(complaint_id)
            else:
                print(f"Invalid: {complaint_id}")
                consecutive_invalid += 1
                failed_ids.add(complaint_id)

                # Exit after 75 consecutive invalid
                if consecutive_invalid >= 75:
                    break
                
                # Skip to next hundred after 50 consecutive invalid
                if consecutive_invalid >= 50:
                    complaint_id = ((complaint_id // 100) + 1) * 100
                    print(f"Skipping to {complaint_id}")
                    continue
            
        except Exception as e:
            print(f"Error {complaint_id}: {e}")
        
        time.sleep(0.1)
        complaint_id += 1

if __name__ == "__main__":
    fetch_complaint_details() 
