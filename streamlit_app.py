import streamlit as st
import pandas as pd
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
from datetime import datetime

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
    def __init__(self):
        self.base_openfda_url = "https://api.fda.gov"
        self.dailymed_base_url = "https://dailymed.nlm.nih.gov/dailymed"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FDA-Research-Tool/1.0 (research@fda.gov)'
        })

        # Initialize empty databases
        self.fei_database = {}
        self.duns_database = {}
        self.database_loaded = False
        
        # Auto-load database
        self.load_database_automatically()

    def load_database_automatically(self):
        """Automatically load database from repository or GitHub"""
        try:
            # Try multiple possible locations for the database file
            possible_files = [
                "drls_reg.xlsx",  # Same directory as app
                "data/drls_reg.xlsx",  # Data subdirectory
                "./drls_reg.xlsx",  # Explicit current directory
                "../drls_reg.xlsx"  # Parent directory
            ]
            
            # Try local files first
            for file_path in possible_files:
                if os.path.exists(file_path):
                    self.load_fei_database_from_spreadsheet(file_path)
                    if self.fei_database or self.duns_database:
                        self.database_loaded = True
                        return
            
            # If no local file found, show error
            st.error("❌ Could not load establishment database from any source")
            
        except Exception as e:
            st.error(f"❌ Error during database loading: {str(e)}")

    def load_fei_database_from_spreadsheet(self, file_path: str):
        """Load FEI and DUNS database from a spreadsheet with FEI_NUMBER, DUNS_NUMBER, ADDRESS, and FIRM_NAME columns"""
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

            # Look for FEI_NUMBER, DUNS_NUMBER, ADDRESS, and FIRM_NAME columns (case insensitive, flexible matching)
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
        
        # Special handling for numbers that might have been stored with/without leading zeros
        if clean_id.startswith('00'):
            # For numbers starting with 00, try removing different amounts of leading zeros
            variants.append(clean_id[1:])  # Remove one zero
            variants.append(clean_id[2:])  # Remove two zeros
        elif clean_id.startswith('0'):
            # For numbers starting with 0, try removing the leading zero
            variants.append(clean_id[1:])
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys([v for v in variants if v]))

    def parse_address(self, address: str) -> Dict:
        """Parse address string into components"""
        try:
            # Basic address parsing - you can enhance this based on your data format
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
            lines = address.replace('\\n', ',').split(',')
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
            postal_match = re.search(r'\\b(\\d{5}(?:-\\d{4})?|\\d{4,6})\\b', address)
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
        """Validate NDC format - more flexible to accept various formats"""
        ndc = str(ndc).strip()
        
        # Remove any non-digit, non-dash characters
        clean_ndc = re.sub(r'[^\d\-]', '', ndc)
        
        # Check if it's a valid NDC format
        patterns = [
            r'^\d{4,5}-\d{3,4}-\d{1,2}$',  # Standard format with dashes
            r'^\d{10,11}$',                 # All digits, 10 or 11 digits
            r'^\d{8,9}$'                    # Sometimes shorter formats exist
        ]
        
        # Also accept if it becomes valid after normalization
        if any(re.match(pattern, clean_ndc) for pattern in patterns):
            return True
        
        # Try to normalize and see if it becomes valid
        try:
            normalized = self.normalize_ndc(clean_ndc)
            return any(re.match(pattern, normalized) for pattern in patterns[:2])
        except:
            pass
        
        # Accept any string of 8-11 digits
        digits_only = re.sub(r'[^\d]', '', ndc)
        return len(digits_only) >= 8 and len(digits_only) <= 11

    def normalize_ndc(self, ndc: str) -> str:
        """Normalize NDC to standard format - more flexible"""
        # Remove any non-digit, non-dash characters
        clean_ndc = re.sub(r'[^\d\-]', '', str(ndc))
        
        # If it already has dashes and is valid, return as-is
        if '-' in clean_ndc:
            # Check if it's already in valid format
            if re.match(r'^\d{4,5}-\d{3,4}-\d{1,2}$', clean_ndc):
                return clean_ndc
            # If dashes are in wrong places, remove them and reformat
            clean_ndc = clean_ndc.replace('-', '')
        
        # Work with digits only
        digits_only = clean_ndc
        
        # Pad to 11 digits if it's 10 digits
        if len(digits_only) == 10:
            digits_only = '0' + digits_only
        elif len(digits_only) == 8:
            digits_only = '000' + digits_only
        elif len(digits_only) == 9:
            digits_only = '00' + digits_only
        
        # Format based on length
        if len(digits_only) == 11:
            return f"{digits_only[:5]}-{digits_only[5:9]}-{digits_only[9:]}"
        elif len(digits_only) == 10:
            return f"{digits_only[:5]}-{digits_only[5:8]}-{digits_only[8:]}"
        else:
            # Return original if we can't format it properly
            return clean_ndc

    def normalize_ndc_for_matching(self, ndc: str) -> List[str]:
        """Generate multiple NDC formats for matching - includes segment conversion"""
        clean_ndc = re.sub(r'[^\d\-]', '', str(ndc))
        variants = set()  # Use set to avoid duplicates
        
        # Remove dashes to get base digits
        digits_only = clean_ndc.replace('-', '')
        
        # Add the original digits
        variants.add(digits_only)
        
        # Add segment conversion variants for 10-digit ↔ 11-digit conversion
        if '-' in clean_ndc:
            parts = clean_ndc.split('-')
            if len(parts) == 3:
                labeler, product, package = parts
                
                # 5-4-2 → 5-3-2 (remove leading zero from product)
                if len(labeler) == 5 and len(product) == 4 and len(package) == 2 and product.startswith('0'):
                    product_unpadded = product[1:]  # Remove first character
                    variants.add(f"{labeler}-{product_unpadded}-{package}")
                    variants.add(f"{labeler}{product_unpadded}{package}")
                    variants.add(f"{labeler}-{product_unpadded}")  # Base format
                    variants.add(f"{labeler}{product_unpadded}")
                
                # 5-3-2 → 5-4-2 (add leading zero to product)
                elif len(labeler) == 5 and len(product) == 3 and len(package) == 2:
                    product_padded = '0' + product  # Add leading zero
                    variants.add(f"{labeler}-{product_padded}-{package}")
                    variants.add(f"{labeler}{product_padded}{package}")
                    variants.add(f"{labeler}-{product_padded}")  # Base format
                    variants.add(f"{labeler}{product_padded}")
        
        # Generate different length versions (original logic)
        if len(digits_only) == 8:
            variants.add('000' + digits_only)  # 11 digits
            variants.add('00' + digits_only)   # 10 digits
            variants.add('0' + digits_only)    # 9 digits
        elif len(digits_only) == 9:
            variants.add('00' + digits_only)   # 11 digits
            variants.add('0' + digits_only)    # 10 digits
            variants.add(digits_only[1:])      # 8 digits (remove leading zero)
        elif len(digits_only) == 10:
            variants.add('0' + digits_only)    # 11 digits
            variants.add(digits_only[1:])      # 9 digits (remove leading zero)
            variants.add(digits_only[2:])      # 8 digits (remove two leading zeros)
        elif len(digits_only) == 11:
            variants.add(digits_only[1:])      # 10 digits (remove leading zero)
            variants.add(digits_only[2:])      # 9 digits (remove two leading zeros)
            variants.add(digits_only[3:])      # 8 digits (remove three leading zeros)
        
        # Generate formatted versions for each variant
        formatted_variants = set()
        for variant in variants:
            if len(variant) == 11:
                formatted_variants.add(f"{variant[:5]}-{variant[5:9]}-{variant[9:]}")
            elif len(variant) == 10:
                formatted_variants.add(f"{variant[:5]}-{variant[5:8]}-{variant[8:]}")
            elif len(variant) == 9:
                formatted_variants.add(f"{variant[:4]}-{variant[4:7]}-{variant[7:]}")
            elif len(variant) == 8:
                formatted_variants.add(f"{variant[:4]}-{variant[4:6]}-{variant[6:]}")
        
        # Combine all variants
        all_variants = variants.union(formatted_variants)
        
        # Add base NDC variants (labeler-product without package)
        base_variants = set()
        for variant in formatted_variants:
            if '-' in variant:
                parts = variant.split('-')
                if len(parts) == 3:  # Standard NDC format
                    base_ndc = f"{parts[0]}-{parts[1]}"  # Remove package part
                    base_variants.add(base_ndc)
                    
                    # Also add base NDC without dashes
                    base_ndc_no_dash = f"{parts[0]}{parts[1]}"
                    base_variants.add(base_ndc_no_dash)
        
        all_variants = all_variants.union(base_variants)
        
        # Convert to list and remove empty strings
        return [v for v in all_variants if v and len(v) >= 6]

    def extract_labeler_from_product_name(self, product_name: str) -> str:
        """Extract labeler name from product name - enhanced extraction"""
        try:
            # Method 1: Look for text in brackets at the end
            bracket_match = re.search(r'\[([^\]]+)\]\s*$', product_name)
            if bracket_match:
                labeler = bracket_match.group(1).strip()
                if labeler and labeler.lower() not in ['unknown', 'n/a', 'none']:
                    return labeler
            
            # Method 2: Look for any brackets in the product name
            all_brackets = re.findall(r'\[([^\]]+)\]', product_name)
            if all_brackets:
                # Take the last bracketed text (usually the manufacturer)
                labeler = all_brackets[-1].strip()
                if labeler and labeler.lower() not in ['unknown', 'n/a', 'none']:
                    return labeler
            
            # Method 3: Look for text after "by" or "from"
            by_match = re.search(r'\b(?:by|from)\s+([^,\[\]]+)', product_name, re.IGNORECASE)
            if by_match:
                labeler = by_match.group(1).strip()
                if labeler and labeler.lower() not in ['unknown', 'n/a', 'none']:
                    return labeler
            
            return 'Not specified'
            
        except Exception as e:
            return 'Not specified'

    def get_ndc_info_comprehensive(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from multiple sources"""
        # Try DailyMed first
        dailymed_info = self.get_ndc_info_from_dailymed(ndc)
        if dailymed_info:
            return dailymed_info

        # Try openFDA as fallback
        openfda_info = self.get_ndc_info_from_openfda(ndc)
        if openfda_info:
            return openfda_info

        return None

    def get_ndc_info_from_dailymed(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from DailyMed with improved labeler extraction"""
        try:
            # Generate comprehensive list of NDC variants
            ndc_variants = self.normalize_ndc_for_matching(ndc)
            
            # Also try the original and basic normalizations
            additional_variants = [
                ndc.replace('-', ''),
                ndc,
                self.normalize_ndc(ndc),
                self.normalize_ndc_11digit(ndc),
                self.normalize_ndc_10digit(ndc)
            ]
            
            # Combine and deduplicate
            all_variants = list(set(ndc_variants + additional_variants))
            
            # Try each variant
            for ndc_variant in all_variants:
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
                            product_name = spl_data.get('title', 'Unknown')
                            
                            # Try multiple methods to get labeler
                            labeler_name = None
                            
                            # Method 1: From API labeler field
                            api_labeler = spl_data.get('labeler', '').strip()
                            if api_labeler and api_labeler not in ['Unknown', '', 'None']:
                                labeler_name = api_labeler
                            
                            # Method 2: Extract from product name
                            if not labeler_name:
                                labeler_name = self.extract_labeler_from_product_name(product_name)
                            
                            # Method 3: Try to get from SPL XML directly
                            if labeler_name in ['Not specified', 'Unknown', ''] and spl_data.get('setid'):
                                spl_labeler, _ = self.extract_labeler_from_spl(spl_data.get('setid'))
                                if spl_labeler and spl_labeler != 'Unknown':
                                    labeler_name = spl_labeler
                            
                            # Final fallback
                            if not labeler_name or labeler_name in ['Unknown', 'Not specified', '']:
                                labeler_name = 'Manufacturer name not available'
                            
                            return ProductInfo(
                                ndc=ndc,  # Return original NDC as entered
                                product_name=product_name,
                                labeler_name=labeler_name,
                                spl_id=spl_data.get('setid')
                            )
                except Exception as e:
                    continue
                    
        except Exception as e:
            pass

        return None

    def get_ndc_info_from_openfda(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from openFDA - try more variants"""
        try:
            # Generate comprehensive list of NDC variants
            ndc_variants = self.normalize_ndc_for_matching(ndc)
            
            # Also try the original and basic normalizations
            additional_variants = [
                ndc.replace('-', ''),
                ndc,
                self.normalize_ndc(ndc),
                self.normalize_ndc_11digit(ndc),
                self.normalize_ndc_10digit(ndc)
            ]
            
            # Combine and deduplicate
            all_variants = list(set(ndc_variants + additional_variants))

            for ndc_variant in all_variants:
                if not ndc_variant or len(ndc_variant) < 6:
                    continue
                    
                try:
                    url = f"{self.base_openfda_url}/drug/label.json"
                    params = {'search': f'openfda.product_ndc:"{ndc_variant}"', 'limit': 1}
                    response = self.session.get(url, params=params)

                    if response.status_code == 200:
                        data = response.json()
                        if data.get('results'):
                            result = data['results'][0]
                            openfda = result.get('openfda', {})

                            brand_names = openfda.get('brand_name', [])
                            generic_names = openfda.get('generic_name', [])
                            manufacturer_names = openfda.get('manufacturer_name', [])

                            product_name = (brand_names[0] if brand_names else
                                          generic_names[0] if generic_names else 'Unknown')
                            labeler_name = manufacturer_names[0] if manufacturer_names else 'Unknown'

                            return ProductInfo(ndc=ndc, product_name=product_name, labeler_name=labeler_name)
                except Exception as e:
                    continue
        except Exception as e:
            pass

        return None

    def normalize_ndc_11digit(self, ndc: str) -> str:
        """Convert NDC to 11-digit format"""
        clean_ndc = ndc.replace('-', '')
        return '0' + clean_ndc if len(clean_ndc) == 10 else clean_ndc

    def normalize_ndc_10digit(self, ndc: str) -> str:
        """Convert NDC to 10-digit format"""
        clean_ndc = ndc.replace('-', '')
        return clean_ndc[1:] if len(clean_ndc) == 11 and clean_ndc.startswith('0') else clean_ndc

    def lookup_fei_establishment(self, fei_number: str) -> Optional[Dict]:
        """Look up establishment information using FEI number from spreadsheet database"""
        try:
            # Try EXPANDED formats for FEI lookup
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
        """Look up establishment information using DUNS number from spreadsheet database"""
        try:
            # Try EXPANDED formats for DUNS lookup
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
        """Find FEI and DUNS numbers in SPL that match the spreadsheet database and return their XML locations"""
        matches = []
        
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return matches

            content = response.text
            
            # Parse XML to get proper structure
            try:
                root = ET.fromstring(content)
                
                # Find all ID elements and check their context
                for elem in root.iter():
                    if elem.tag.endswith('id') and elem.get('extension'):
                        extension = elem.get('extension')
                        root_oid = elem.get('root', '')
                        
                        # Clean the extension (remove non-digits)
                        clean_extension = re.sub(r'[^\d]', '', extension)
                        
                        # Get XML location/context information
                        xml_context = self._get_element_context(elem, root)
                        xml_location = self._get_element_xpath(elem, root)
                        
                        # Check if this is an FEI number match (try EXPANDED formats)
                        fei_match_found = False
                        fei_variants = self._generate_all_id_variants(extension)
                        
                        for fei_key in fei_variants:
                            if fei_key in self.fei_database:
                                establishment_name = self._extract_establishment_name_from_context(elem)
                                
                                match = FEIMatch(
                                    fei_number=clean_extension,
                                    xml_location=xml_location,
                                    match_type='FEI_NUMBER',
                                    establishment_name=establishment_name,
                                    xml_context=xml_context
                                )
                                matches.append(match)
                                fei_match_found = True
                                break
                        
                        # Check if this is a DUNS number match (try EXPANDED formats)
                        if not fei_match_found:
                            duns_variants = self._generate_all_id_variants(extension)
                            
                            for duns_key in duns_variants:
                                if duns_key in self.duns_database:
                                    establishment_name = self._extract_establishment_name_from_context(elem)
                                    
                                    match = FEIMatch(
                                        fei_number=clean_extension,  # Using same field for both FEI and DUNS
                                        xml_location=xml_location,
                                        match_type='DUNS_NUMBER',
                                        establishment_name=establishment_name,
                                        xml_context=xml_context
                                    )
                                    matches.append(match)
                                    break
                            
            except ET.XMLSyntaxError as e:
                # Fallback to regex-based approach
                matches.extend(self._find_matches_with_regex(content, spl_id))
                
        except Exception as e:
            pass
            
        return matches

    def _get_element_xpath(self, element, root) -> str:
        """Generate XPath-like location for an element"""
        try:
            path_parts = []
            current = element
            
            # Build path by walking up the tree
            while current is not None and current != root:
                tag = current.tag.split('}')[-1] if '}' in current.tag else current.tag
                
                # Count siblings with same tag to get position
                parent = current.getparent() if hasattr(current, 'getparent') else None
                if parent is not None:
                    siblings = [sibling for sibling in parent if sibling.tag == current.tag]
                    if len(siblings) > 1:
                        index = siblings.index(current) + 1
                        path_parts.insert(0, f"{tag}[{index}]")
                    else:
                        path_parts.insert(0, tag)
                else:
                    path_parts.insert(0, tag)
                    
                current = parent
                
            return "/" + "/".join(path_parts) if path_parts else "unknown_xpath"
        except Exception as e:
            return "xpath_error"

    def _get_element_context(self, element, root) -> str:
        """Get surrounding context for an element"""
        try:
            context_parts = []
            
            # Get parent element information
            parent = element.getparent() if hasattr(element, 'getparent') else None
            if parent is not None:
                parent_tag = parent.tag.split('}')[-1] if '}' in parent.tag else parent.tag
                context_parts.append(f"Parent: {parent_tag}")
                
                # Look for name elements in parent
                for child in parent:
                    child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    if 'name' in child_tag.lower() and child.text:
                        context_parts.append(f"Name: {child.text.strip()}")
                        break
            
            # Get element attributes
            attrs = []
            for key, value in element.attrib.items():
                key_clean = key.split('}')[-1] if '}' in key else key
                attrs.append(f"{key_clean}='{value}'")
            
            if attrs:
                context_parts.append(f"Attributes: {', '.join(attrs)}")
                
            return " | ".join(context_parts)
        except Exception as e:
            return "context_unavailable"

    def _extract_establishment_name_from_context(self, element) -> str:
        """Extract establishment name from XML context"""
        try:
            # Look for name elements in parent or nearby elements
            parent = element.getparent() if hasattr(element, 'getparent') else None
            if parent is not None:
                # Look for name elements
                for child in parent:
                    child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    if 'name' in child_tag.lower() and child.text:
                        return child.text.strip()
                        
                # Look in grandparent
                grandparent = parent.getparent() if hasattr(parent, 'getparent') else None
                if grandparent is not None:
                    for child in grandparent:
                        child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        if 'name' in child_tag.lower() and child.text:
                            return child.text.strip()
                            
            return "Unknown"
        except Exception as e:
            return "Unknown"

    def _find_matches_with_regex(self, content: str, spl_id: str) -> List[FEIMatch]:
        """Fallback regex-based matching with location information"""
        matches = []
        
        try:
            # Find all ID elements with extension attributes
            id_pattern = r'<id\s+([^>]*extension="(\d{7,15})"[^>]*)>'
            id_matches = re.finditer(id_pattern, content, re.IGNORECASE)
            
            for match in id_matches:
                full_match = match.group(0)
                extension = match.group(2)
                clean_extension = re.sub(r'[^\d]', '', extension)
                
                # Calculate line number for location
                line_num = content[:match.start()].count('\n') + 1
                
                # Get surrounding context (100 chars before and after)
                start_context = max(0, match.start() - 100)
                end_context = min(len(content), match.end() + 100)
                xml_context = content[start_context:end_context].replace('\n', ' ').strip()
                
                xml_location = f"Line {line_num} (regex-based)"
                
                # Check for FEI matches (try EXPANDED formats)
                fei_match_found = False
                fei_variants = self._generate_all_id_variants(extension)
                
                for fei_key in fei_variants:
                    if fei_key in self.fei_database:
                        fei_match = FEIMatch(
                            fei_number=clean_extension,
                            xml_location=xml_location,
                            match_type='FEI_NUMBER',
                            establishment_name=self._extract_name_from_context_regex(xml_context),
                            xml_context=xml_context[:200] + "..." if len(xml_context) > 200 else xml_context
                        )
                        matches.append(fei_match)
                        fei_match_found = True
                        break
                
                # Check for DUNS matches (try EXPANDED formats)
                if not fei_match_found:
                    duns_variants = self._generate_all_id_variants(extension)
                    
                    for duns_key in duns_variants:
                        if duns_key in self.duns_database:
                            duns_match = FEIMatch(
                                fei_number=clean_extension,
                                xml_location=xml_location,
                                match_type='DUNS_NUMBER',
                                establishment_name=self._extract_name_from_context_regex(xml_context),
                                xml_context=xml_context[:200] + "..." if len(xml_context) > 200 else xml_context
                            )
                            matches.append(duns_match)
                            break
                    
        except Exception as e:
            pass
            
        return matches

    def _extract_name_from_context_regex(self, context: str) -> str:
        """Extract establishment name from context using regex"""
        try:
            # Look for name tags
            name_match = re.search(r'<name[^>]*>([^<]+)</name>', context, re.IGNORECASE)
            if name_match:
                return name_match.group(1).strip()
            return "Unknown"
        except Exception as e:
            return "Unknown"

    def extract_ndc_specific_operations(self, section: str, target_ndc: str, establishment_name: str) -> Tuple[List[str], List[str]]:
        """Extract operations that are specific to the target NDC from an establishment section"""
        operations = []
        quotes = []

        # Generate all possible NDC variants for matching
        ndc_variants = self.normalize_ndc_for_matching(target_ndc)

        # Updated operation mappings
        operation_codes = {
            'C43360': 'Manufacture',
            'C82401': 'Manufacture', 
            'C25391': 'Analysis',
            'C84731': 'Pack',
            'C25392': 'Label',
            'C48482': 'Repack',
            'C73606': 'Relabel',
            'C84732': 'Sterilize',
            'C25394': 'API Manufacture',
            'C43359': 'Manufacture'
        }

        # Look for performance elements with actDefinition (this is the correct structure for SPL)
        performance_elements = re.findall(r'<performance[^>]*>.*?</performance>', section, re.DOTALL | re.IGNORECASE)

        for perf_elem in performance_elements:
            # Extract operation code and displayName from actDefinition
            operation_found = None
            operation_code_match = re.search(r'<code[^>]*code="([^"]*)"[^>]*displayName="([^"]*)"', perf_elem, re.IGNORECASE)
            
            if operation_code_match:
                operation_code = operation_code_match.group(1)
                
                # Map operation code to our standard operation names
                if operation_code in operation_codes:
                    operation_found = operation_codes[operation_code]

            if operation_found:
                # Look for NDC codes in manufacturedMaterialKind
                ndc_code_pattern = r'<code[^>]*code="([^"]*)"[^>]*codeSystem="2\.16\.840\.1\.113883\.6\.69"'
                ndc_matches = re.findall(ndc_code_pattern, perf_elem, re.IGNORECASE)
                
                ndc_found_in_operation = False
                for ndc_code in ndc_matches:
                    # Clean up the NDC code
                    clean_ndc = ndc_code.strip()
                    
                    # Generate variants for this NDC code
                    potential_variants = self.normalize_ndc_for_matching(clean_ndc)

                    # Check if any variant matches our target NDC
                    matching_variants = [v for v in potential_variants if v in ndc_variants]
                    if matching_variants:
                        ndc_found_in_operation = True
                        break

                # If our target NDC was found in this operation, add it
                if ndc_found_in_operation and operation_found not in operations:
                    operations.append(operation_found)
                    quotes.append(f'"Found {operation_found} operation for National Drug Code {target_ndc} in {establishment_name}"')

        # Remove "Manufacture" if "API Manufacture" is present
        if 'API Manufacture' in operations and 'Manufacture' in operations:
            operations.remove('Manufacture')
            quotes = [q for q in quotes if 'Manufacture operation' not in q or 'API Manufacture operation' in q]

        return operations, quotes

    def extract_general_operations(self, section: str, establishment_name: str) -> Tuple[List[str], List[str]]:
        """Extract general operations from an establishment section (not NDC-specific)"""
        operations = []
        quotes = []

        # Updated operation mappings
        operation_codes = {
            'C43360': 'Manufacture',
            'C82401': 'Manufacture', 
            'C25391': 'Analysis',
            'C84731': 'Pack',
            'C25392': 'Label',
            'C48482': 'Repack',
            'C73606': 'Relabel',
            'C84732': 'Sterilize',
            'C25394': 'API Manufacture',
            'C43359': 'Manufacture'
        }

        operation_names = {
            'manufacture': 'Manufacture',
            'api manufacture': 'API Manufacture',
            'analysis': 'Analysis',
            'label': 'Label',
            'pack': 'Pack',
            'repack': 'Repack',
            'relabel': 'Relabel',
            'sterilize': 'Sterilize'
        }

        # Look for business operations
        business_operations = re.findall(r'<businessOperation[^>]*>.*?</businessOperation>', section, re.DOTALL | re.IGNORECASE)

        for bus_op in business_operations:
            operation_found = None

            # Check for displayName attributes
            display_name_match = re.search(r'displayName="([^"]*)"', bus_op, re.IGNORECASE)
            if display_name_match:
                display_name = display_name_match.group(1).lower()
                if 'api' in display_name and 'manufacture' in display_name:
                    operation_found = 'API Manufacture'
                else:
                    for name, operation in operation_names.items():
                        if name in display_name and operation != 'API Manufacture':
                            operation_found = operation
                            break

            # Check for operation codes
            if not operation_found:
                for code, operation in operation_codes.items():
                    if code in bus_op:
                        operation_found = operation
                        break

            if operation_found and operation_found not in operations:
                operations.append(operation_found)
                quotes.append(f'Found {operation_found} operation in {establishment_name}')

        # Remove "Manufacture" if "API Manufacture" is present
        if 'API Manufacture' in operations and 'Manufacture' in operations:
            operations.remove('Manufacture')
            quotes = [q for q in quotes if 'Manufacture operation' not in q or 'API Manufacture operation' in q]

        return operations, quotes

    def extract_establishments_with_fei(self, spl_id: str, target_ndc: str) -> Tuple[List[str], List[str], List[Dict]]:
        """Extract operations, quotes, and detailed establishment info with FEI/DUNS numbers for specific NDC"""
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return [], [], []

            content = response.text
            establishments_info = []
            processed_numbers = set()  # Track processed FEI/DUNS numbers to avoid duplicates

            # First, find FEI/DUNS matches with their XML locations
            matches = self.find_fei_duns_matches_in_spl(spl_id)
            
            # Get establishment sections for operation extraction
            establishment_sections = re.findall(r'<assignedEntity[^>]*>.*?</assignedEntity>', content, re.DOTALL | re.IGNORECASE)
            
            for match in matches:
                # Skip if we've already processed this number
                if match.fei_number in processed_numbers:
                    continue
                
                processed_numbers.add(match.fei_number)
                
                # Look up establishment info based on match type
                if match.match_type == 'FEI_NUMBER':
                    establishment_info = self.lookup_fei_establishment(match.fei_number)
                else:  # DUNS_NUMBER
                    establishment_info = self.lookup_duns_establishment(match.fei_number)
                
                if establishment_info:
                    # Find the establishment section that contains our matched number and extract operations
                    establishment_operations = []
                    establishment_quotes = []
                    establishment_included = False
                    
                    # Look for this FEI/DUNS in establishment sections to get operations
                    for section in establishment_sections:
                        # Check if this section contains our matched number
                        if match.fei_number in section:
                            # Extract establishment name from section
                            name_match = re.search(r'<name[^>]*>([^<]+)</name>', section)
                            section_establishment_name = name_match.group(1) if name_match else establishment_info.get('establishment_name', 'Unknown')
                            
                            # Extract NDC-specific operations for this establishment
                            ops, quotes = self.extract_ndc_specific_operations(section, target_ndc, section_establishment_name)
                            
                            # MODIFIED LOGIC: Include establishment if:
                            # 1. We found NDC-specific operations, OR
                            # 2. No NDC-specific operations found but establishment has business operations (less strict fallback)
                            if ops:
                                # Found NDC-specific operations
                                establishment_operations.extend(ops)
                                establishment_quotes.extend(quotes)
                                establishment_included = True
                            else:
                                # Fallback: Check if establishment has any business operations at all
                                all_business_ops = re.findall(r'<businessOperation[^>]*>.*?</businessOperation>', section, re.DOTALL | re.IGNORECASE)
                                if all_business_ops:
                                    # Extract general operations (not NDC-specific)
                                    general_ops, general_quotes = self.extract_general_operations(section, section_establishment_name)
                                    if general_ops:
                                        establishment_operations.extend(general_ops)
                                        establishment_quotes.extend([f"General operation (not National Drug Code-specific): {q}" for q in general_quotes])
                                        establishment_included = True
                            
                            # Only process the FIRST matching section to avoid duplicates
                            break
                    
                    # Add establishment if we found operations (either NDC-specific or general)
                    if establishment_included:
                        # Add match location information
                        establishment_info['xml_location'] = match.xml_location
                        establishment_info['match_type'] = match.match_type
                        establishment_info['xml_context'] = match.xml_context
                        
                        # Remove duplicates while preserving order
                        establishment_operations = list(dict.fromkeys(establishment_operations))
                        establishment_quotes = list(dict.fromkeys(establishment_quotes))
                        
                        establishment_info['operations'] = establishment_operations
                        establishment_info['quotes'] = establishment_quotes
                        
                        establishments_info.append(establishment_info)

            # Return empty lists for document-level operations since we now have establishment-specific ones
            return [], [], establishments_info

        except Exception as e:
            return [], [], []

    def get_establishment_info(self, product_info: ProductInfo) -> List[Dict]:
        """Get establishment information for a product with NDC-specific operations"""
        establishments = []

        # Extract company names
        company_names = self.extract_company_names(product_info)

        # Create establishments from SPL data with NDC-specific operations
        establishments = self.create_establishments_from_spl(company_names, product_info)

        return establishments[:10]  # Limit to 10 establishments to avoid too many results

    def extract_company_names(self, product_info: ProductInfo) -> List[str]:
        """Extract company names from product information"""
        company_names = []

        # Extract from product name (text in brackets)
        bracket_matches = re.findall(r'\[([^\]]+)\]', product_info.product_name)
        for match in bracket_matches:
            clean_match = re.sub(r'\s+(INC|LLC|CORP|LTD|CO\.?|COMPANY)\.?$', '', match, flags=re.IGNORECASE)
            if len(clean_match) > 3:
                company_names.append(clean_match.strip())

        # Add labeler name
        if product_info.labeler_name and product_info.labeler_name != 'Unknown':
            company_names.append(product_info.labeler_name)

        return company_names

    def create_establishments_from_spl(self, company_names: List[str], product_info: ProductInfo) -> List[Dict]:
        """Create multiple establishments based on SPL data with NDC-specific operations"""
        establishments = []

        if not product_info or not product_info.spl_id:
            return establishments

        # Get operations and establishment info from SPL for the specific NDC
        _, _, establishments_info = self.extract_establishments_with_fei(product_info.spl_id, product_info.ndc)

        if not establishments_info:
            # Try to find labeler info and create a labeler entry
            labeler_info = self.find_labeler_info_in_spl(product_info.spl_id, product_info.labeler_name)
            if labeler_info:
                establishments.append(labeler_info)
        else:
            # Use the establishments found in SPL with their specific operations
            establishments = establishments_info

        return establishments

    def extract_labeler_from_spl(self, spl_id: str) -> Tuple[str, str]:
        """Extract labeler name and DUNS from SPL"""
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return "Unknown", None

            content = response.text
            
            # Parse XML to find labeler information
            try:
                root = ET.fromstring(content)
                
                # Look for author section which typically contains labeler information
                for elem in root.iter():
                    if 'author' in elem.tag.lower():
                        labeler_name = None
                        labeler_duns = None
                        
                        # Look for representedOrganization within author
                        for child in elem.iter():
                            if 'representedOrganization' in child.tag.lower() or 'organization' in child.tag.lower():
                                # Look for name
                                for name_elem in child.iter():
                                    if name_elem.tag.endswith('name') and name_elem.text:
                                        labeler_name = name_elem.text.strip()
                                        break
                                
                                # Look for ID (DUNS)
                                for id_elem in child.iter():
                                    if id_elem.tag.endswith('id') and id_elem.get('extension'):
                                        extension = id_elem.get('extension')
                                        clean_extension = re.sub(r'[^\d]', '', extension)
                                        if len(clean_extension) >= 8:  # Looks like DUNS
                                            labeler_duns = clean_extension
                                            break
                                
                                if labeler_name:
                                    return labeler_name, labeler_duns
                
                # Fallback: look for any organization name in the document
                org_name_pattern = r'<name[^>]*>([^<]+(?:Inc|LLC|Corp|Company|Ltd)[^<]*)</name>'
                name_matches = re.findall(org_name_pattern, content, re.IGNORECASE)
                if name_matches:
                    return name_matches[0].strip(), None
                    
            except ET.XMLSyntaxError:
                # Fallback to regex-based approach
                # Look for labeler name in author sections
                author_pattern = r'<author[^>]*>.*?<representedOrganization[^>]*>.*?<name[^>]*>([^<]+)</name>.*?</representedOrganization>.*?</author>'
                author_matches = re.findall(author_pattern, content, re.DOTALL | re.IGNORECASE)
                if author_matches:
                    return author_matches[0].strip(), None
                
                # Look for any organization name
                org_pattern = r'<name[^>]*>([^<]+(?:Inc|LLC|Corp|Company|Ltd)[^<]*)</name>'
                org_matches = re.findall(org_pattern, content, re.IGNORECASE)
                if org_matches:
                    return org_matches[0].strip(), None
            
            return "Unknown", None
                
        except Exception as e:
            return "Unknown", None

    def find_labeler_info_in_spl(self, spl_id: str, labeler_name: str) -> Optional[Dict]:
        """Find labeler information from SPL with enhanced fallback"""
        try:
            # First, extract the actual labeler name and DUNS from SPL
            actual_labeler_name, labeler_duns = self.extract_labeler_from_spl(spl_id)
            
            # Use the extracted name if available, otherwise use the provided name
            if actual_labeler_name != "Unknown":
                labeler_name = actual_labeler_name
            
            # Try to find DUNS information if we have it
            if labeler_duns:
                duns_info = self.lookup_duns_establishment(labeler_duns)
                if duns_info:
                    return {
                        'establishment_name': duns_info.get('establishment_name', labeler_name),
                        'firm_name': duns_info.get('firm_name', labeler_name),
                        'address_line_1': duns_info.get('address_line_1', 'Unknown'),
                        'city': duns_info.get('city', 'Unknown'),
                        'state_province': duns_info.get('state_province', 'Unknown'),
                        'country': duns_info.get('country', 'Unknown'),
                        'postal_code': duns_info.get('postal_code', ''),
                        'latitude': duns_info.get('latitude'),
                        'longitude': duns_info.get('longitude'),
                        'search_method': 'labeler_duns_database',
                        'duns_number': labeler_duns,
                        'match_type': 'LABELER'
                    }
            
            # Fallback: Return basic labeler info with the actual name from SPL
            return {
                'establishment_name': labeler_name,
                'firm_name': labeler_name,
                'address_line_1': 'Address not available in official documentation',
                'city': 'Unknown',
                'state_province': 'Unknown',
                'country': 'Unknown',
                'postal_code': '',
                'latitude': None,
                'longitude': None,
                'search_method': 'labeler_name_from_spl',
                'duns_number': labeler_duns,
                'match_type': 'LABELER'
            }
                
        except Exception as e:
            # Final fallback: Return basic labeler info
            return {
                'establishment_name': labeler_name,
                'firm_name': labeler_name,
                'address_line_1': 'Address not available',
                'city': 'Unknown',
                'state_province': 'Unknown',
                'country': 'Unknown',
                'postal_code': '',
                'latitude': None,
                'longitude': None,
                'search_method': 'labeler_name_only',
                'match_type': 'LABELER'
            }

    def process_single_ndc(self, ndc: str) -> pd.DataFrame:
        """Process a single NDC number"""
        if not self.validate_ndc_format(ndc):
            return pd.DataFrame()

        normalized_ndc = self.normalize_ndc(ndc)

        product_info = self.get_ndc_info_comprehensive(normalized_ndc)
        if not product_info:
            return pd.DataFrame()

        establishments = self.get_establishment_info(product_info)

        results = []
        if establishments:
            for establishment in establishments:
                results.append({
                    'ndc': ndc,
                    'product_name': product_info.product_name,
                    'labeler_name': product_info.labeler_name,
                    'spl_id': product_info.spl_id,
                    'fei_number': establishment.get('fei_number'),
                    'duns_number': establishment.get('duns_number'),
                    'establishment_name': establishment.get('establishment_name'),
                    'firm_name': establishment.get('firm_name'),
                    'address_line_1': establishment.get('address_line_1'),
                    'city': establishment.get('city'),
                    'state': establishment.get('state_province'),
                    'country': establishment.get('country'),
                    'postal_code': establishment.get('postal_code', ''),
                    'latitude': establishment.get('latitude'),
                    'longitude': establishment.get('longitude'),
                    'spl_operations': ', '.join(establishment.get('operations', [])) if establishment.get('operations') else 'None found for this National Drug Code',
                    'spl_quotes': ' | '.join(establishment.get('quotes', [])),
                    'search_method': establishment.get('search_method'),
                    'xml_location': establishment.get('xml_location', 'Unknown'),
                    'match_type': establishment.get('match_type', 'Unknown'),
                    'xml_context': establishment.get('xml_context', '')
                })
        else:
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

def generate_multi_location_google_maps_link(results_df: pd.DataFrame) -> str:
    """Generate Google Maps link with multiple location pins"""
    try:
        # Get all valid addresses
        locations = []
        for _, row in results_df.iterrows():
            if (row['address_line_1'] and 
                'not available' not in str(row['address_line_1']).lower() and 
                row['address_line_1'] != 'Unknown' and
                row['city'] != 'Unknown'):
                
                address_parts = []
                if row['establishment_name'] and row['establishment_name'] != 'Unknown':
                    address_parts.append(row['establishment_name'])
                if row['address_line_1'] != 'Unknown':
                    address_parts.append(row['address_line_1'])
                if row['city'] != 'Unknown':
                    address_parts.append(row['city'])
                if row['state'] != 'Unknown':
                    address_parts.append(row['state'])
                if row['country'] != 'Unknown':
                    address_parts.append(row['country'])
                
                if address_parts:
                    full_address = ', '.join(address_parts)
                    locations.append(full_address)
        
        if not locations:
            return None
        
        # Create multi-location Google Maps URL
        if len(locations) == 1:
            # Single location
            encoded_address = locations[0].replace(' ', '+').replace(',', '%2C').replace('&', '%26')
            return f"https://www.google.com/maps/search/{encoded_address}"
        else:
            # Multiple locations - use Google Maps directions with waypoints
            origin = locations[0]
            destination = locations[-1]
            waypoints = locations[1:-1] if len(locations) > 2 else []
            
            origin_encoded = origin.replace(' ', '+').replace(',', '%2C').replace('&', '%26')
            dest_encoded = destination.replace(' ', '+').replace(',', '%2C').replace('&', '%26')
            
            if waypoints:
                waypoints_encoded = '|'.join([wp.replace(' ', '+').replace(',', '%2C').replace('&', '%26') for wp in waypoints])
                return f"https://www.google.com/maps/dir/{origin_encoded}/{waypoints_encoded}/{dest_encoded}"
            else:
                return f"https://www.google.com/maps/dir/{origin_encoded}/{dest_encoded}"
                
    except Exception as e:
        return None

def generate_individual_google_maps_link(row) -> str:
    """Generate Google Maps link for a single establishment location"""
    # Skip if no valid address information
    if (row['match_type'] == 'LABELER' and 
        ('Address not available' in str(row['address_line_1']) or 
         not row['address_line_1'] or 
         row['address_line_1'] == 'Unknown')):
        return None
        
    # Build address for this establishment
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
    
    # Create Google Maps search URL for this specific location
    full_address = ', '.join(address_parts)
    encoded_address = full_address.replace(' ', '+').replace(',', '%2C').replace('&', '%26')
    return f"https://www.google.com/maps/search/{encoded_address}"

def main():
    st.set_page_config(
        page_title="Medication Manufacturing Location Lookup", 
        page_icon="💊",
        layout="wide"
    )
    
    st.title("💊 Medication Manufacturing Location Lookup")
    st.markdown("### Find where your medications are manufactured")
    st.markdown("Enter a National Drug Code number to discover manufacturing establishments, locations, and operations using FDA data.")
    
    # Auto-load database and show status (simplified)
    if 'mapper' not in st.session_state:
        st.session_state.mapper = NDCToLocationMapper()
            
    if not st.session_state.mapper.database_loaded:
        st.error("❌ Could not load establishment database")
        st.stop()
    
    # Input section with Enter key functionality
    st.markdown("---")
    
    # Use form to enable Enter key submission
    with st.form("ndc_search_form"):
        col1, col2 = st.columns([3, 1])
        with col1:
            ndc_input = st.text_input(
                "Enter National Drug Code Number:", 
                placeholder="Example: 0185-0674-01",
                help="National Drug Code format: 12345-678-90 or 1234567890"
            )
        with col2:
            st.write("")  # Spacing
            search_btn = st.form_submit_button("🔍 Search", type="primary")
    
    # Example NDCs
    st.markdown("**Try these examples:**")
    examples = ["0185-0674-01", "50242-061-10", "63323-262-06", "63323-459-14"]
    cols = st.columns(len(examples))
    for i, ex in enumerate(examples):
        with cols[i]:
            if st.button(f"`{ex}`", key=f"ex_{i}"):
                ndc_input = ex
                search_btn = True
    
    # Information links
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("📖 [How to find your medication's National Drug Code](https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory)")
    with col2:
        st.markdown("📋 [Understanding manufacturing operations](https://www.fda.gov/drugs/pharmaceutical-quality-resources/pharmaceutical-quality-resources)")
    
    # Search functionality
    if search_btn and ndc_input:
        with st.spinner(f"Looking up manufacturing locations for {ndc_input}..."):
            try:
                results_df = st.session_state.mapper.process_single_ndc(ndc_input)
                
                if len(results_df) > 0:
                    first_row = results_df.iloc[0]
                    
                    if first_row['search_method'] == 'no_establishments_found':
                        st.warning(f"⚠️ Found product information but no manufacturing establishments detected")
                        
                        # Product name on its own line
                        st.markdown(f"**📦 Product:**")
                        st.markdown(f"{first_row['product_name']}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("National Drug Code", first_row['ndc'])
                        with col2:
                            st.metric("Manufacturer/Labeler", first_row['labeler_name'])
                            
                        if first_row['spl_id']:
                            spl_url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={first_row['spl_id']}"
                            st.markdown(f"📄 **Official Product Document:** [View on DailyMed]({spl_url})")
                        
                        st.info("💡 This product may not have detailed establishment information in its official documentation (~70% of products don't include manufacturing details), or the establishments may not be in our database.")
                    
                    else:
                        # Full results with establishments
                        st.success(f"✅ Found {len(results_df)} manufacturing establishments")
                        
                        # Product name on its own line
                        st.markdown(f"**📦 Product:**")
                        st.markdown(f"{first_row['product_name']}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("National Drug Code", first_row['ndc'])
                        with col2:
                            st.metric("Manufacturer/Labeler", first_row['labeler_name'])
                            
                        if first_row['spl_id']:
                            spl_url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={first_row['spl_id']}"
                            st.markdown(f"📄 **Official Product Document:** [View on DailyMed]({spl_url})")
                        
                        # Country distribution
                        if len(results_df) > 1:
                            country_counts = results_df['country'].value_counts()
                            country_summary = ", ".join([f"{country}: {count}" for country, count in country_counts.items()])
                            st.markdown(f"🌍 **Manufacturing Countries:** {country_summary}")
                        
                        # Multi-location Google Maps link
                        multi_maps_link = generate_multi_location_google_maps_link(results_df)
                        if multi_maps_link:
                            st.markdown(f"🗺️ **[View all locations on Google Maps]({multi_maps_link})**")
                        
                        # Manufacturing establishments
                        st.subheader(f"🏭 Manufacturing Establishments ({len(results_df)})")
                        
                        for idx, row in results_df.iterrows():
                            with st.expander(f"Establishment {idx + 1}: {row['establishment_name']}", expanded=True):
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    if row['fei_number']:
                                        st.write(f"**🔢 FDA Establishment Identifier:** {row['fei_number']}")
                                    if row['duns_number']:
                                        st.write(f"**🔢 Business Identifier:** {row['duns_number']}")
                                    if row['firm_name'] and row['firm_name'] != 'Unknown':
                                        st.write(f"**🏢 Company Name:** {row['firm_name']}")
                                
                                with col2:
                                    if row['country'] and row['country'] != 'Unknown':
                                        st.write(f"**🌍 Country:** {row['country']}")
                                    if row['spl_operations'] and row['spl_operations'] != 'None found for this National Drug Code':
                                        st.write(f"**⚙️ Manufacturing Operations:** {row['spl_operations']}")
                                
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
                    st.error(f"❌ No results found for: {ndc_input}")
                    st.info("💡 This National Drug Code may not exist in the FDA database. Please check the format and try again.")
                    
            except Exception as e:
                st.error(f"❌ Error processing request: {str(e)}")
                with st.expander("Technical Details"):
                    st.exception(e)
    
    # Sidebar info
    st.sidebar.title("About This Tool")
    st.sidebar.markdown("""
    This tool helps you find where your medications are manufactured by:
    
    🔍 **Looking up your medication** in FDA databases  
    📄 **Analyzing official documents** for manufacturing info  
    🏭 **Finding manufacturing facilities** worldwide  
    🌍 **Showing locations** on interactive maps  
    
    **What you can discover:**
    - ✅ Where your medication is made
    - ✅ What company manufactures it  
    - ✅ Manufacturing operations performed
    - ✅ Global supply chain information
    - ✅ Interactive maps of facilities
    
    **Coverage:**
    Approximately 30% of medications have detailed manufacturing establishment information available in their official FDA documentation.
    """)
    
    if 'mapper' in st.session_state and st.session_state.mapper.database_loaded:
        st.sidebar.markdown("---")
        st.sidebar.metric("FDA Database Entries", f"{len(st.session_state.mapper.fei_database):,}")
        st.sidebar.metric("Business Database Entries", f"{len(st.session_state.mapper.duns_database):,}")
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Database Status:**")
        st.sidebar.success("✅ Loaded and Ready")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**⚠️ Important Disclaimer:**")
    st.sidebar.markdown("""
    This tool is provided for **informational and educational purposes only**. 
    
    - Information may not be complete or current
    - Not intended for medical decision-making
    - Does not replace consultation with healthcare providers
    - Manufacturing

