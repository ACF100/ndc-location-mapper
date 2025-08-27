import streamlit as st
import pandas as pd
import tempfile
import os
import requests
import json
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
import re
import warnings

logging.basicConfig(level=logging.ERROR)

@dataclass
class ProductInfo:
    ndc: str
    product_name: str
    labeler_name: str
    spl_id: Optional[str] = None

@dataclass  
class FEIMatch:
    fei_number: str
    xml_location: str
    match_type: str
    establishment_name: str = None
    xml_context: str = None

class NDCToLocationMapper:
    def __init__(self, fei_spreadsheet_path: str = None):
        self.base_openfda_url = "https://api.fda.gov"
        self.dailymed_base_url = "https://dailymed.nlm.nih.gov/dailymed"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FDA-Research-Tool/1.0 (research@fda.gov)'
        })
        self.fei_database = {}
        self.duns_database = {}
        if fei_spreadsheet_path:
            self.load_fei_database_from_spreadsheet(fei_spreadsheet_path)

    def load_fei_database_from_spreadsheet(self, file_path: str):
        try:
            df = pd.read_excel(file_path, dtype=str)
            fei_count = 0
            duns_count = 0
            
            for idx, row in df.iterrows():
                try:
                    address = str(row['ADDRESS']).strip()
                    if pd.isna(row['ADDRESS']) or address == 'nan' or address == '':
                        continue
                    
                    firm_name = str(row['FIRM_NAME']).strip() if not pd.isna(row['FIRM_NAME']) else 'Unknown'
                    
                    if not pd.isna(row['FEI_NUMBER']):
                        fei_number = str(row['FEI_NUMBER']).strip()
                        if fei_number != 'nan' and fei_number != '':
                            fei_clean = re.sub(r'[^\d]', '', fei_number)
                            if len(fei_clean) >= 7:
                                establishment_data = {
                                    'establishment_name': firm_name,
                                    'firm_name': firm_name,
                                    'address_line_1': address,
                                    'city': 'Unknown',
                                    'state_province': 'Unknown', 
                                    'country': 'Unknown',
                                    'postal_code': '',
                                    'latitude': None,
                                    'longitude': None,
                                    'search_method': 'spreadsheet_fei_database',
                                    'original_fei': fei_number
                                }
                                possible_keys = self._generate_all_id_variants(fei_number)
                                for key in possible_keys:
                                    if key:
                                        self.fei_database[key] = establishment_data
                                fei_count += 1
                    
                    if not pd.isna(row['DUNS_NUMBER']):
                        duns_number = str(row['DUNS_NUMBER']).strip()
                        if duns_number != 'nan' and duns_number != '':
                            duns_clean = re.sub(r'[^\d]', '', duns_number)
                            if len(duns_clean) >= 8:
                                establishment_data = {
                                    'establishment_name': firm_name,
                                    'firm_name': firm_name,
                                    'address_line_1': address,
                                    'city': 'Unknown',
                                    'state_province': 'Unknown',
                                    'country': 'Unknown', 
                                    'postal_code': '',
                                    'latitude': None,
                                    'longitude': None,
                                    'search_method': 'spreadsheet_duns_database',
                                    'original_duns': duns_number
                                }
                                possible_keys = self._generate_all_id_variants(duns_number)
                                for key in possible_keys:
                                    if key:
                                        self.duns_database[key] = establishment_data
                                duns_count += 1
                except Exception as e:
                    continue
        except Exception as e:
            pass

    def _generate_all_id_variants(self, id_number: str):
        clean_id = re.sub(r'[^\d]', '', str(id_number))
        variants = [str(id_number).strip(), clean_id]
        try:
            id_as_int = int(clean_id)
            for padding in [8, 9, 10, 11, 12, 13, 14, 15]:
                padded = f"{id_as_int:0{padding}d}"
                variants.append(padded)
            variants.append(str(id_as_int))
        except ValueError:
            pass
        return list(dict.fromkeys([v for v in variants if v]))

    def get_ndc_info_from_dailymed(self, ndc: str):
        try:
            ndc_variants = [ndc, ndc.replace('-', '')]
            for ndc_variant in ndc_variants:
                if not ndc_variant or len(ndc_variant) < 6:
                    continue
                try:
                    search_url = f"{self.dailymed_base_url}/services/v2/spls.json"
                    params = {'ndc': ndc_variant, 'page_size': 1}
                    response = self.session.get(search_url, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('data'):
                            spl_data = data['data'][0]
                            return ProductInfo(
                                ndc=ndc,
                                product_name=spl_data.get('title', 'Unknown'),
                                labeler_name=spl_data.get('labeler', 'Unknown'),
                                spl_id=spl_data.get('setid')
                            )
                except Exception as e:
                    continue
        except Exception as e:
            pass
        return None

    def process_single_ndc(self, ndc: str):
        product_info = self.get_ndc_info_from_dailymed(ndc)
        if not product_info:
            return pd.DataFrame()
        
        # Simplified version for now - just return basic info
        results = [{
            'ndc': ndc,
            'product_name': product_info.product_name,
            'labeler_name': product_info.labeler_name,
            'spl_id': product_info.spl_id,
            'fei_number': None,
            'duns_number': None,
            'establishment_name': 'Basic lookup only',
            'firm_name': product_info.labeler_name,
            'address_line_1': 'Full functionality available in complete version',
            'city': 'Unknown',
            'state': 'Unknown', 
            'country': 'Unknown',
            'postal_code': '',
            'latitude': None,
            'longitude': None,
            'spl_operations': 'Full SPL analysis in complete version',
            'spl_quotes': '',
            'search_method': 'basic_lookup'
        }]
        return pd.DataFrame(results)

def main():
    st.title("💊 NDC Manufacturing Location Lookup")
    
    uploaded_file = st.file_uploader("Upload DRLS Registry File", type=['xlsx'])
    
    if uploaded_file is not None:
        if 'mapper' not in st.session_state:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                st.session_state.mapper = NDCToLocationMapper(tmp_file.name)
                os.unlink(tmp_file.name)
            st.success("Database loaded!")
        
        ndc_input = st.text_input("Enter NDC Number:", placeholder="50242-061-01")
        
        if st.button("🔍 Search") and ndc_input:
            with st.spinner("Searching..."):
                results_df = st.session_state.mapper.process_single_ndc(ndc_input)
                if len(results_df) > 0:
                    st.success(f"Found product information")
                    st.dataframe(results_df)
                else:
                    st.error("No results found")
    else:
        st.info("Please upload your DRLS registry file")

if __name__ == "__main__":
    main()
