#!/usr/bin/env python3

import pandas as pd
import requests
import time

from pathlib import Path
from bs4 import BeautifulSoup

def load_ids(file_path, data_key=None, msg_prefix=""):
    """Generic function to load IDs from file."""
    if not Path(file_path).exists():
        print(f"No {file_path} found, {msg_prefix}")
        return set()
    
    try:
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
            ids = set(int(str(cid)) for cid in df[data_key].dropna())
        elif file_path.endswith('.txt'):
            with open(file_path, 'r') as f:
                ids = set(int(line.strip()) for line in f if line.strip())
        
        print(f"Loaded {len(ids)} IDs from {file_path}")
        return ids
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return set()

def save_failed_ids(failed_ids):
    """Save failed complaint IDs to .failed.txt file."""
    try:
        with open(".failed.txt", 'w') as f:
            f.write('\n'.join(str(cid) for cid in sorted(failed_ids)) + '\n')
    except Exception as e:
        print(f"Error saving .failed.txt: {e}")

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
    if complaint_id in existing_ids:
        print(f"Skipping {complaint_id} - in parquet")
        return True
    if complaint_id in failed_ids:
        print(f"Skipping {complaint_id} - failed")
        return True
    if Path(f"raw/{complaint_id}.html").exists():
        print(f"Skipping {complaint_id} - file exists")
        return True
    return False

def fetch_complaint_details():
    """Fetch complaint details for complaint IDs not in parquet file."""
    existing_ids = load_ids("data/combined.parquet", "complaint_id", "will fetch all complaints")
    failed_ids = load_ids(".failed.txt", msg_prefix="will check all complaint IDs")
    
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
    
    while complaint_id <= 20800000:
        if should_skip(complaint_id, existing_ids, failed_ids):
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
                else:
                    print(f"No panel: {complaint_id}")
                    consecutive_invalid += 1
            else:
                print(f"Invalid: {complaint_id}")
                failed_ids.add(complaint_id)
                consecutive_invalid += 1
                
                # Skip to next thousand after 10 consecutive invalid
                if consecutive_invalid >= 10:
                    complaint_id = ((complaint_id // 1000) + 1) * 1000
                    print(f"Skipping to {complaint_id}")
                    save_failed_ids(failed_ids)
                    consecutive_invalid = 0
                    continue
            
        except Exception as e:
            print(f"Error {complaint_id}: {e}")
        
        time.sleep(0.5)
        complaint_id += 1

if __name__ == "__main__":
    fetch_complaint_details() 