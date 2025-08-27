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

# Configure logging to only show errors
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

@dataclass
class ProductInfo:
    ndc: str
    product_name: str
    labeler_name: str
    spl_id: Optional[str] = None
    fei_numbers: List[str] = None
    establishments: List[Dict] = None

@dataclass
class FEIMatch:
    fei_number: str
    xml_location: str
    match_type: str  # 'FEI_NUMBER' or 'DUNS_NUMBER'
    establishment_name: str = None
    xml_context: str = None  # Surrounding XML context

class NDCToLocationMapper:
    def __init__(self, fei_spreadsheet_path: str = None):
        self.base_openfda_url = "https://api.fda.gov"
        self.dailymed_base_url = "https://dailymed.nlm.nih.gov/dailymed"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FDA-Research-Tool/1.0 (research@fda.gov)'
        })

        # Load FEI database from spreadsheet
        self.fei_database = {}
        self.duns_database = {}  # Add DUNS database
        if fei_spreadsheet_path:
            self.load_fei_database_from_spreadsheet(fei_spreadsheet_path)

    def load_fei_database_from_spreadsheet(self, file_path: str):
        """Load FEI and DUNS database from a spreadsheet"""
        try:
            # Try to read the file with different engines
            try:
                # Force all columns to be read as strings to preserve leading zeros
                df = pd.read_excel(file_path, dtype=str)
            except:
                try:
                    df = pd.read_csv(file_path, dtype=str)
                except Exception as e:
                    return

            # Look for FEI_NUMBER, DUNS_NUMBER, ADDRESS, and FIRM_NAME columns
            fei_col = None
            duns_col = None
            address_col = None
            firm_name_col = None

            for col in df.columns:
                col_lower = col.lower().strip().replace('_', '').replace(' ', '')
                col_original = col.strip()
                
                # More flexible FEI column matching
                if ('fei' in col_lower and 'number' in col_lower) or col_lower == 'feinumber':
                    fei_col = col_original
                # More flexible DUNS column matching
                elif ('duns' in col_lower and 'number' in col_lower) or col_lower == 'dunsnumber':
                    duns_col = col_original
                # More flexible ADDRESS column matching
                elif 'address' in col_lower:
                    address_col = col_original
                # More flexible FIRM_NAME column matching
                elif ('firm' in col_lower and 'name' in col_lower) or col_lower == 'firmname':
                    firm_name_col = col_original

            if not fei_col and not duns_col:
                return

            if not address_col:
                return

            # Process each row
            fei_count = 0
            duns_count = 0
            
            for idx, row in df.iterrows():
                try:
                    address = str(row[address_col]).strip()
                    
                    # Skip empty address rows
                    if pd.isna(row[address_col]) or address == 'nan' or address == '':
                        continue

                    # Parse address components
                    address_parts = self.parse_address(address)
                    
                    # Get firm name if available
                    firm_name = 'Unknown'
                    if firm_name_col and not pd.isna(row[firm_name_col]):
                        firm_name = str(row[firm_name_col]).strip()
                        if firm_name == 'nan' or firm_name == '':
                            firm_name = 'Unknown'

                    # Process FEI number if column exists
                    if fei_col and not pd.isna(row[fei_col]):
                        fei_number = str(row[fei_col]).strip()
                        if fei_number != 'nan' and fei_number != '':
                            # Clean FEI number (remove any non-digits)
                            fei_clean = re.sub(r'[^\d]', '', fei_number)

                            if len(fei_clean) >= 7:  # Valid FEI numbers are typically 7-10 digits
                                # Store in FEI database with multiple key formats
                                establishment_data = {
                                    'establishment_name': address_parts.get('establishment_name', 'Unknown'),
                                    'firm_name': firm_name,
                                    'address_line_1': address_parts.get('address_line_1', address),
                                    'city': address_parts.get('city', 'Unknown'),
                                    'state_province': address_parts.get('state_province', 'Unknown'),
                                    'country': address_parts.get('country', 'Unknown'),
                                    'postal_code': address_parts.get('postal_code', ''),
                                    'latitude': address_parts.get('latitude'),
                                    'longitude': address_parts.get('longitude'),
                                    'search_method': 'spreadsheet_fei_database',
                                    'original_fei': fei_number
                                }
                                
                                # Generate ALL possible key formats for FEI
                                possible_keys = self._generate_all_id_variants(fei_number)
                                
                                for key in possible_keys:
                                    if key:
                                        self.fei_database[key] = establishment_data
                                        
                                fei_count += 1

                    # Process DUNS number if column exists
                    if duns_col and not pd.isna(row[duns_col]):
                        duns_number = str(row[duns_col]).strip()
                        if duns_number != 'nan' and duns_number != '':
                            # Handle DUNS numbers that may be stored as text with leading zeros
                            duns_clean = re.sub(r'[^\d]', '', duns_number)

                            if len(duns_clean) >= 8:  # Valid DUNS numbers are typically 9 digits
                                # Store in DUNS database
                                establishment_data = {
                                    'establishment_name': address_parts.get('establishment_name', 'Unknown'),
                                    'firm_name': firm_name,
                                    'address_line_1': address_parts.get('address_line_1', address),
                                    'city': address_parts.get('city', 'Unknown'),
                                    'state_province': address_parts.get('state_province', 'Unknown'),
                                    'country': address_parts.get('country', 'Unknown'),
                                    'postal_code': address_parts.get('postal_code', ''),
                                    'latitude': address_parts.get('latitude'),
                                    'longitude': address_parts.get('longitude'),
                                    'search_method': 'spreadsheet_duns_database',
                                    'original_duns': duns_number
                                }
                                
                                # Generate ALL possible key formats for DUNS
                                possible_keys = self._generate_all_id_variants(duns_number)
                                
                                # Store under all possible key formats
                                for key in possible_keys:
                                    if key:  # Make sure key is not empty
                                        self.duns_database[key] = establishment_data
                                        
                                duns_count += 1

                except Exception as e:
                    continue

        except Exception as e:
            pass

    def _generate_all_id_variants(self, id_number: str) -> List[str]:
        """Generate all possible variants of an ID number for matching"""
        clean_id = re.sub(r'[^\d]', '', str(id_number))
        variants = []
        
        # Add original formats
        variants.extend([
            str(id_number).strip(),
            clean_id,
            clean_id.lstrip('0')
        ])
        
        # Add numeric conversion variants
        try:
            id_as_int = int(clean_id)
            # Add padded versions for different lengths
            for padding in [8, 9, 10, 11, 12, 13, 14, 15]:
                padded = f"{id_as_int:0{padding}d}"
                variants.append(padded)
            
            # Add string of int
            variants.append(str(id_as_int))
            
        except ValueError:
            pass
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys([v for v in variants if v]))

    def parse_address(self, address: str) -> Dict:
        """Parse address string into components"""
        try:
            # Basic address parsing
            parts = {
                'establishment_name': 'Unknown',
                'address_line_1': address,
                'city': 'Unknown',
                'state_province': 'Unknown',
                'country': 'Unknown',
                'postal_code': '',
                'latitude': None,
                'longitude': None
            }

            # Try to extract establishment name (first line before comma or newline)
            lines = address.replace('\n', ',').split(',')
            if len(lines) > 0:
                parts['establishment_name'] = lines[0].strip()

            # Try to extract city, state, country from last parts
            if len(lines) >= 2:
                parts['address_line_1'] = lines[1].strip() if len(lines) > 1 else lines[0].strip()

            if len(lines) >= 3:
                # Look for city in second to last part
                city_part = lines[-2].strip()
                parts['city'] = city_part

            if len(lines) >= 4:
                # Look for state/country in last part
                last_part = lines[-1].strip()
                parts['state_province'] = last_part

                # Common country patterns
                if any(country in last_part.upper() for country in ['USA', 'US', 'UNITED STATES']):
                    parts['country'] = 'USA'
                elif any(country in last_part.upper() for country in ['GERMANY', 'DEUTSCHLAND']):
                    parts['country'] = 'Germany'
                elif any(country in last_part.upper() for country in ['SWITZERLAND', 'SCHWEIZ']):
                    parts['country'] = 'Switzerland'
                elif any(country in last_part.upper() for country in ['SINGAPORE']):
                    parts['country'] = 'Singapore'
                else:
                    parts['country'] = last_part

            # Extract postal code (look for patterns like 12345 or 12345-6789)
            postal_match = re.search(r'\b(\d{5}(?:-\d{4})?|\d{4,6})\b', address)
            if postal_match:
                parts['postal_code'] = postal_match.group(1)

            return parts

        except Exception as e:
            return {
                'establishment_name': 'Unknown',
                'address_line_1': address,
                'city': 'Unknown',
                'state_province': 'Unknown',
                'country': 'Unknown',
                'postal_code': '',
                'latitude': None,
                'longitude': None
            }

    def validate_ndc_format(self, ndc: str) -> bool:
        """Validate NDC format"""
        ndc = str(ndc).strip()
        clean_ndc = re.sub(r'[^\d\-]', '', ndc)
        
        patterns = [
            r'^\d{4,5}-\d{3,4}-\d{1,2}$',
            r'^\d{10,11}$',
            r'^\d{8,9}$'
        ]
        
        if any(re.match(pattern, clean_ndc) for pattern in patterns):
            return True
        
        try:
            normalized = self.normalize_ndc(clean_ndc)
            return any(re.match(pattern, normalized) for pattern in patterns[:2])
        except:
            pass
        
        digits_only = re.sub(r'[^\d]', '', ndc)
        return len(digits_only) >= 8 and len(digits_only) <= 11

    def normalize_ndc(self, ndc: str) -> str:
        """Normalize NDC to standard format"""
        clean_ndc = re.sub(r'[^\d\-]', '', str(ndc))
        
        if '-' in clean_ndc:
            if re.match(r'^\d{4,5}-\d{3,4}-\d{1,2}$', clean_ndc):
                return clean_ndc
            clean_ndc = clean_ndc.replace('-', '')
        
        digits_only = clean_ndc
        
        if len(digits_only) == 10:
            digits_only = '0' + digits_only
        elif len(digits_only) == 8:
            digits_only = '000' + digits_only
        elif len(digits_only) == 9:
            digits_only = '00' + digits_only
        
        if len(digits_only) == 11:
            return f"{digits_only[:5]}-{digits_only[5:9]}-{digits_only[9:]}"
        elif len(digits_only) == 10:
            return f"{digits_only[:5]}-{digits_only[5:8]}-{digits_only[8:]}"
        else:
            return clean_ndc

    def get_ndc_info_from_dailymed(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from DailyMed"""
        try:
            ndc_variants = [ndc, ndc.replace('-', ''), self.normalize_ndc(ndc)]
            
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

    def get_ndc_info_comprehensive(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from multiple sources"""
        dailymed_info = self.get_ndc_info_from_dailymed(ndc)
        if dailymed_info:
            return dailymed_info
        return None

    def lookup_fei_establishment(self, fei_number: str) -> Optional[Dict]:
        """Look up establishment information using FEI number"""
        try:
            fei_variants = self._generate_all_id_variants(fei_number)

            for fei_variant in fei_variants:
                if fei_variant in self.fei_database:
                    establishment_info = self.fei_database[fei_variant].copy()
                    establishment_info['fei_number'] = fei_variant
                    return establishment_info
                    
            return None
        except Exception as e:
            return None

    def lookup_duns_establishment(self, duns_number: str) -> Optional[Dict]:
        """Look up establishment information using DUNS number"""
        try:
            duns_variants = self._generate_all_id_variants(duns_number)

            for duns_variant in duns_variants:
                if duns_variant in self.duns_database:
                    establishment_info = self.duns_database[duns_variant].copy()
                    establishment_info['duns_number'] = duns_variant
                    return establishment_info
                    
            return None
        except Exception as e:
            return None

    def find_fei_duns_matches_in_spl(self, spl_id: str) -> List[FEIMatch]:
        """Find FEI and DUNS numbers in SPL that match the spreadsheet database"""
        matches = []
        
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return matches

            content = response.text
            
            # Find all ID elements with extension attributes
            id_pattern = r'<id\s+([^>]*extension="(\d{7,15})"[^>]*)'
            id_matches = re.findall(id_pattern, content, re.IGNORECASE)
            
            for full_match, extension in id_matches:
                clean_extension = re.sub(r'[^\d]', '', extension)
                
                # Check if this is an FEI number match
                fei_match_found = False
                fei_variants = self._generate_all_id_variants(extension)
                
                for fei_key in fei_variants:
                    if fei_key in self.fei_database:
                        establishment_name = self.fei_database[fei_key].get('establishment_name', 'Unknown')
                        
                        match = FEIMatch(
                            fei_number=clean_extension,
                            xml_location="SPL Document",
                            match_type='FEI_NUMBER',
                            establishment_name=establishment_name
                        )
                        matches.append(match)
                        fei_match_found = True
                        break
                
                # Check if this is a DUNS number match
                if not fei_match_found:
                    duns_variants = self._generate_all_id_variants(extension)
                    
                    for duns_key in duns_variants:
                        if duns_key in self.duns_database:
                            establishment_name = self.duns_database[duns_key].get('establishment_name', 'Unknown')
                            
                            match = FEIMatch(
                                fei_number=clean_extension,
                                xml_location="SPL Document",
                                match_type='DUNS_NUMBER',
                                establishment_name=establishment_name
                            )
                            matches.append(match)
                            break
                            
        except Exception as e:
            pass
            
        return matches

    def process_single_ndc(self, ndc: str) -> pd.DataFrame:
        """Process a single NDC number with full functionality"""
        if not self.validate_ndc_format(ndc):
            return pd.DataFrame()

        normalized_ndc = self.normalize_ndc(ndc)
        product_info = self.get_ndc_info_comprehensive(normalized_ndc)
        
        if not product_info:
            return pd.DataFrame()

        results = []
        
        # Get establishments from SPL
        if product_info.spl_id:
            matches = self.find_fei_duns_matches_in_spl(product_info.spl_id)
            
            for match in matches:
                if match.match_type == 'FEI_NUMBER':
                    establishment_info = self.lookup_fei_establishment(match.fei_number)
                else:
                    establishment_info = self.lookup_duns_establishment(match.fei_number)
                
                if establishment_info:
                    results.append({
                        'ndc': ndc,
                        'product_name': product_info.product_name,
                        'labeler_name': product_info.labeler_name,
                        'spl_id': product_info.spl_id,
                        'fei_number': establishment_info.get('fei_number') if match.match_type == 'FEI_NUMBER' else None,
                        'duns_number': establishment_info.get('duns_number') if match.match_type == 'DUNS_NUMBER' else None,
                        'establishment_name': establishment_info.get('establishment_name', 'Unknown'),
                        'firm_name': establishment_info.get('firm_name', 'Unknown'),
                        'address_line_1': establishment_info.get('address_line_1', 'Unknown'),
                        'city': establishment_info.get('city', 'Unknown'),
                        'state': establishment_info.get('state_province', 'Unknown'),
                        'country': establishment_info.get('country', 'Unknown'),
                        'postal_code': establishment_info.get('postal_code', ''),
                        'latitude': establishment_info.get('latitude'),
                        'longitude': establishment_info.get('longitude'),
                        'spl_operations': 'Analysis, Manufacture',  # Simplified for now
                        'spl_quotes': '',
                        'search_method': establishment_info.get('search_method', 'database_lookup'),
                        'xml_location': match.xml_location,
                        'match_type': match.match_type,
                        'xml_context': ''
                    })
        
        # If no establishments found, return basic product info
        if not results:
            results.append({
                'ndc': ndc,
                'product_name': product_info.product_name,
                'labeler_name': product_info.labeler_name,
                'spl_id': product_info.spl_id,
                'fei_number': None,
                'duns_number': None,
                'establishment_name': None,
                'firm_name': None,
                'address_line_1': None,
                'city': None,
                'state': None,
                'country': None,
                'postal_code': '',
                'latitude': None,
                'longitude': None,
                'spl_operations': None,
                'spl_quotes': None,
                'search_method': 'no_establishments_found',
                'xml_location': None,
                'match_type': None,
                'xml_context': ''
            })

        return pd.DataFrame(results)

def generate_individual_google_maps_link(row) -> str:
    """Generate Google Maps link for a single establishment location"""
    if (row['match_type'] == 'LABELER' and 
        ('Address not available' in str(row['address_line_1']) or 
         not row['address_line_1'] or 
         row['address_line_1'] == 'Unknown')):
        return None
        
    address_parts = []
    if row['establishment_name'] and row['establishment_name'] != 'Unknown':
        address_parts.append(row['establishment_name'])
    if row['address_line_1'] and row['address_line_1'] != 'Unknown':
        address_parts.append(row['address_line_1'])
    if row['city'] and row['city'] != 'Unknown':
        address_parts.append(row['city'])
    if row['state'] and row['state'] != 'Unknown':
        address_parts.append(row['state'])
    if row['postal_code']:
        address_parts.append(row['postal_code'])
    if row['country'] and row['country'] != 'Unknown':
        address_parts.append(row['country'])
    
    if not address_parts:
        return None
    
    full_address = ', '.join(address_parts)
    encoded_address = full_address.replace(' ', '+').replace(',', '%2C').replace('&', '%26')
    return f"https://www.google.com/maps/search/{encoded_address}"

def main():
    st.set_page_config(
        page_title="NDC Manufacturing Location Lookup", 
        page_icon="💊",
        layout="wide"
    )
    
    st.title("💊 NDC Manufacturing Location Lookup")
    st.markdown("### Find where your medications are manufactured")
    st.markdown("Upload your establishment database and enter an NDC number to discover manufacturing locations and operations.")
    
    # File upload for database
    uploaded_file = st.file_uploader(
        "Upload DRLS Registry File", 
        type=['xlsx'],
        help="Upload the Excel file containing FEI and DUNS establishment data"
    )
    
    if uploaded_file is not None:
        # Initialize mapper with uploaded file
        if 'mapper' not in st.session_state:
            with st.spinner("Loading establishment database..."):
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    st.session_state.mapper = NDCToLocationMapper(tmp_file.name)
                    os.unlink(tmp_file.name)
            
            if st.session_state.mapper.fei_database or st.session_state.mapper.duns_database:
                st.success(f"✅ Database loaded: {len(st.session_state.mapper.fei_database):,} FEI entries, {len(st.session_state.mapper.duns_database):,} DUNS entries")
            else:
                st.error("❌ Could not load database from file")
                return
        
        # NDC input
        col1, col2 = st.columns([3, 1])
        with col1:
            ndc_input = st.text_input(
                "Enter NDC Number:", 
                placeholder="50242-061-01",
                help="NDC format: 12345-678-90 or 1234567890"
            )
        with col2:
            search_btn = st.button("🔍 Search", type="primary")
        
        # Example NDCs
        st.markdown("**Try these examples:**")
        examples = ["50242-061-01", "0093-7663-56", "0781-1506-10"]
        cols = st.columns(len(examples))
        for i, ex in enumerate(examples):
            with cols[i]:
                if st.button(f"`{ex}`", key=f"ex_{i}"):
                    ndc_input = ex
                    search_btn = True
        
        if search_btn and ndc_input:
            with st.spinner(f"Looking up manufacturing locations for {ndc_input}..."):
                try:
                    results_df = st.session_state.mapper.process_single_ndc(ndc_input)
                    
                    if len(results_df) > 0:
                        first_row = results_df.iloc[0]
                        
                        # Check if establishments were found
                        if first_row['search_method'] == 'no_establishments_found':
                            st.warning(f"⚠️ Found product information for NDC {ndc_input}, but no manufacturing establishments detected")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Product Name", first_row['product_name'])
                                st.metric("NDC Number", first_row['ndc'])
                            with col2:
                                labeler = first_row['labeler_name'] if first_row['labeler_name'] != 'Unknown' else 'Not specified'
                                st.metric("Labeler", labeler)
                                
                                if first_row['spl_id']:
                                    spl_url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={first_row['spl_id']}"
                                    st.markdown(f"📄 **SPL Document:** [View on DailyMed]({spl_url})")
                            
                            st.info("💡 This product may not have detailed establishment information in its SPL document, or the establishments may not be in your database.")
                        
                        else:
                            # Full results with establishments
                            st.success(f"✅ Found {len(results_df)} manufacturing establishments for NDC: {ndc_input}")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Product Name", first_row['product_name'])
                                st.metric("NDC Number", first_row['ndc'])
                            
                            with col2:
                                labeler = first_row['labeler_name'] if first_row['labeler_name'] != 'Unknown' else 'Not specified'
                                st.metric("Labeler", labeler)
                                
                                if first_row['spl_id']:
                                    spl_url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={first_row['spl_id']}"
                                    st.markdown(f"📄 **SPL Document:** [View on DailyMed]({spl_url})")
                            
                            # Country distribution
                            if len(results_df) > 1:
                                country_counts = results_df['country'].value_counts()
                                country_summary = ", ".join([f"{country}: {count}" for country, count in country_counts.items()])
                                st.markdown(f"🌍 **Country Distribution:** {country_summary}")
                            
                            # Manufacturing establishments
                            st.subheader(f"🏭 Manufacturing Establishments ({len(results_df)})")
                            
                            for idx, row in results_df.iterrows():
                                with st.expander(f"Establishment {idx + 1}: {row['establishment_name']}", expanded=True):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        if row['fei_number']:
                                            st.write(f"**🔢 FEI Number:** {row['fei_number']}")
                                        if row['duns_number']:
                                            st.write(f"**🔢 DUNS Number:** {row['duns_number']}")
                                        if row['firm_name'] and row['firm_name'] != 'Unknown':
                                            st.write(f"**🏢 Firm Name:** {row['firm_name']}")
                                    
                                    with col2:
                                        if row['country'] and row['country'] != 'Unknown':
                                            st.write(f"**🌍 Country:** {row['country']}")
                                        if row['spl_operations']:
                                            st.write(f"**⚙️ Operations:** {row['spl_operations']}")
                                    
                                    # Address
                                    if row['address_line_1'] and 'not available' not in str(row['address_line_1']).lower() and row['address_line_1'] != 'Unknown':
                                        address_parts = []
                                        if row['address_line_1'] != 'Unknown':
                                            address_parts.append(row['address_line_1'])
                                        if row['city'] != 'Unknown':
                                            address_parts.append(row['city'])
                                        if row['state'] != 'Unknown':
                                            address_parts.append(row['state'])
                                        if row['postal_code']:
                                            address_parts.append(row['postal_code'])
                                        if row['country'] != 'Unknown':
                                            address_parts.append(row['country'])
                                        
                                        if address_parts:
                                            full_address = ', '.join(address_parts)
                                            st.write(f"**📍 Address:** {full_address}")
                                            
                                            # Google Maps link
                                            maps_link = generate_individual_google_maps_link(row)
                                            if maps_link:
                                                st.markdown(f"🗺️ [View on Google Maps]({maps_link})")
                            
                            # Summary table
                            st.subheader("📊 Summary Table")
                            display_cols = ['establishment_name', 'firm_name', 'country', 'spl_operations']
                            if any(results_df['fei_number'].notna()):
                                display_cols.append('fei_number')
                            if any(results_df['duns_number'].notna()):
                                display_cols.append('duns_number')
                            
                            st.dataframe(results_df[display_cols], use_container_width=True)
                            
                    else:
                        st.error(f"❌ No results found for NDC: {ndc_input}")
                        st.info("💡 This NDC may not exist in DailyMed database. Try checking the NDC format.")
                        
                except Exception as e:
                    st.error(f"❌ Error processing NDC: {str(e)}")
                    with st.expander("Debug Information"):
                        st.exception(e)
        
        # Sidebar info
        st.sidebar.title("About This Tool")
        st.sidebar.markdown("""
        This tool finds manufacturing establishments for NDC numbers by:
        
        🔍 **Looking up the drug** in FDA databases  
        📄 **Analyzing SPL documents** for establishment info  
        🏭 **Matching FEI/DUNS numbers** to locations  
        🌍 **Showing global manufacturing** network  
        
        **Data Sources:**
        - FDA Structured Product Labels (SPL)
        - FDA Establishment Registration Database  
        - DailyMed Database
        - OpenFDA API
        """)
        
        if 'mapper' in st.session_state:
            st.sidebar.markdown("---")
            st.sidebar.metric("FEI Database Entries", f"{len(st.session_state.mapper.fei_database):,}")
            st.sidebar.metric("DUNS Database Entries", f"{len(st.session_state.mapper.duns_database):,}")
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Disclaimer:** For informational purposes only.")
    
    else:
        st.info("👆 Please upload your DRLS registry Excel file to begin")
        st.markdown("**What this tool does:**")
        st.markdown("- 🔍 Looks up NDC numbers in FDA databases")
        st.markdown("- 🏭 Finds manufacturing establishments")
        st.markdown("- 📍 Shows locations and addresses")
        st.markdown("- ⚙️ Displays manufacturing operations")
        st.markdown("- 🌍 Maps global supply chains")

if __name__ == "__main__":
    main()
