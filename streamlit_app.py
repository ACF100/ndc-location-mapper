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
                    st.info(f"📂 Loading establishment database from repository...")
                    self.load_fei_database_from_spreadsheet(file_path)
                    if self.fei_database or self.duns_database:
                        self.database_loaded = True
                        return
            
            # If no local file found, show error
            st.error("❌ Could not load establishment database from any source")
            st.info("💡 Please ensure drls_reg.xlsx is available in the repository")
            
        except Exception as e:
            st.error(f"❌ Error during database loading: {str(e)}")

    def load_fei_database_from_spreadsheet(self, file_path: str):
        """Load FEI and DUNS database from a spreadsheet"""
        try:
            try:
                df = pd.read_excel(file_path, dtype=str)
            except:
                try:
                    df = pd.read_csv(file_path, dtype=str)
                except Exception as e:
                    return

            # Look for columns
            fei_col = None
            duns_col = None
            address_col = None
            firm_name_col = None

            for col in df.columns:
                col_lower = col.lower().strip().replace('_', '').replace(' ', '')
                col_original = col.strip()
                
                if ('fei' in col_lower and 'number' in col_lower) or col_lower == 'feinumber':
                    fei_col = col_original
                elif ('duns' in col_lower and 'number' in col_lower) or col_lower == 'dunsnumber':
                    duns_col = col_original
                elif 'address' in col_lower:
                    address_col = col_original
                elif ('firm' in col_lower and 'name' in col_lower) or col_lower == 'firmname':
                    firm_name_col = col_original

            if not fei_col and not duns_col:
                return
            if not address_col:
                return

            fei_count = 0
            duns_count = 0
            
            for idx, row in df.iterrows():
                try:
                    address = str(row[address_col]).strip()
                    if pd.isna(row[address_col]) or address == 'nan' or address == '':
                        continue

                    address_parts = self.parse_address(address)
                    firm_name = 'Unknown'
                    if firm_name_col and not pd.isna(row[firm_name_col]):
                        firm_name = str(row[firm_name_col]).strip()
                        if firm_name == 'nan' or firm_name == '':
                            firm_name = 'Unknown'

                    # Process FEI number
                    if fei_col and not pd.isna(row[fei_col]):
                        fei_number = str(row[fei_col]).strip()
                        if fei_number != 'nan' and fei_number != '':
                            fei_clean = re.sub(r'[^\d]', '', fei_number)
                            if len(fei_clean) >= 7:
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
                                
                                possible_keys = self._generate_all_id_variants(fei_number)
                                for key in possible_keys:
                                    if key:
                                        self.fei_database[key] = establishment_data
                                fei_count += 1

                    # Process DUNS number
                    if duns_col and not pd.isna(row[duns_col]):
                        duns_number = str(row[duns_col]).strip()
                        if duns_number != 'nan' and duns_number != '':
                            duns_clean = re.sub(r'[^\d]', '', duns_number)
                            if len(duns_clean) >= 8:
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
                                
                                possible_keys = self._generate_all_id_variants(duns_number)
                                for key in possible_keys:
                                    if key:
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
        
        variants.extend([
            str(id_number).strip(),
            clean_id,
            clean_id.lstrip('0')
        ])
        
        try:
            id_as_int = int(clean_id)
            for padding in [8, 9, 10, 11, 12, 13, 14, 15]:
                padded = f"{id_as_int:0{padding}d}"
                variants.append(padded)
            variants.append(str(id_as_int))
        except ValueError:
            pass
        
        return list(dict.fromkeys([v for v in variants if v]))

    def parse_address(self, address: str) -> Dict:
        """Parse address string into components"""
        try:
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

            lines = address.replace('\n', ',').split(',')
            if len(lines) > 0:
                parts['establishment_name'] = lines[0].strip()

            if len(lines) >= 2:
                parts['address_line_1'] = lines[1].strip() if len(lines) > 1 else lines[0].strip()
            if len(lines) >= 3:
                city_part = lines[-2].strip()
                parts['city'] = city_part
            if len(lines) >= 4:
                last_part = lines[-1].strip()
                parts['state_province'] = last_part

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

    def normalize_ndc_for_matching(self, ndc: str) -> List[str]:
        """IMPROVED: Generate comprehensive NDC variants for matching all formats"""
        clean_ndc = re.sub(r'[^\d\-]', '', str(ndc))
        variants = set()
        
        # ALWAYS add the original NDC as-is (this was missing!)
        variants.add(clean_ndc.strip())
        
        # Remove dashes to get base digits
        digits_only = clean_ndc.replace('-', '')
        variants.add(digits_only)
        
        # Add simple variants first
        variants.add(clean_ndc.replace('-', ''))
        variants.add(clean_ndc)
        
        # Handle segment conversion for dashed NDCs
        if '-' in clean_ndc:
            parts = clean_ndc.split('-')
            if len(parts) == 3:
                labeler, product, package = parts
                
                # For 4-4-2 format (like 0185-0674-01), try 5-4-2
                if len(labeler) == 4 and len(product) == 4 and len(package) == 2:
                    new_labeler = '0' + labeler
                    new_format = f"{new_labeler}-{product}-{package}"
                    variants.add(new_format)
                    variants.add(new_format.replace('-', ''))
                
                # For 5-4-2 format, try 4-4-2 
                elif len(labeler) == 5 and len(product) == 4 and len(package) == 2:
                    if labeler.startswith('0'):
                        new_labeler = labeler[1:]
                        new_format = f"{new_labeler}-{product}-{package}"
                        variants.add(new_format)
                        variants.add(new_format.replace('-', ''))
                
                # For 5-3-2, try 5-4-2 (pad product)
                elif len(labeler) == 5 and len(product) == 3 and len(package) == 2:
                    product_padded = '0' + product
                    variants.add(f"{labeler}-{product_padded}-{package}")
                    variants.add(f"{labeler}{product_padded}{package}")
                
                # For 4-3-2, try multiple conversions
                elif len(labeler) == 4 and len(product) == 3 and len(package) == 2:
                    # Try 5-3-2 (pad labeler)
                    labeler_padded = '0' + labeler
                    variants.add(f"{labeler_padded}-{product}-{package}")
                    variants.add(f"{labeler_padded}{product}{package}")
                    
                    # Try 4-4-2 (pad product)
                    product_padded = '0' + product
                    variants.add(f"{labeler}-{product_padded}-{package}")
                    variants.add(f"{labeler}{product_padded}{package}")
        
        # Generate all length variations (comprehensive padding)
        base_digits = digits_only
        for target_length in [8, 9, 10, 11]:
            if len(base_digits) < target_length:
                # Pad with leading zeros
                padded = base_digits.zfill(target_length)
                variants.add(padded)
                
                # Add formatted versions
                if target_length == 11:
                    variants.add(f"{padded[:5]}-{padded[5:9]}-{padded[9:]}")
                elif target_length == 10:
                    variants.add(f"{padded[:5]}-{padded[5:8]}-{padded[8:]}")  # 5-3-2
                    variants.add(f"{padded[:4]}-{padded[4:8]}-{padded[8:]}")  # 4-4-2
                elif target_length == 9:
                    variants.add(f"{padded[:4]}-{padded[4:7]}-{padded[7:]}")  # 4-3-2
            
            elif len(base_digits) > target_length:
                # Remove leading zeros
                trimmed = base_digits.lstrip('0')
                if len(trimmed) == target_length:
                    variants.add(trimmed)
                    
                    # Add formatted versions
                    if target_length == 10:
                        variants.add(f"{trimmed[:5]}-{trimmed[5:8]}-{trimmed[8:]}")
                        variants.add(f"{trimmed[:4]}-{trimmed[4:8]}-{trimmed[8:]}")
                    elif target_length == 9:
                        variants.add(f"{trimmed[:4]}-{trimmed[4:7]}-{trimmed[7:]}")
        
        # Remove empty strings and return unique list
        final_variants = [v for v in variants if v and len(v) >= 6]
        return list(set(final_variants))

    def extract_labeler_from_product_name(self, product_name: str) -> str:
        """Extract labeler name from product name when it's in brackets"""
        try:
            bracket_match = re.search(r'\[([^\]]+)\]\s*$', product_name)
            if bracket_match:
                return bracket_match.group(1).strip()
            
            if ' [' in product_name:
                parts = product_name.split(' [')
                if len(parts) > 1:
                    labeler = parts[-1].replace(']', '').strip()
                    return labeler
                    
            return 'Unknown'
        except:
            return 'Unknown'

    def get_ndc_info_from_dailymed(self, ndc: str) -> Optional[ProductInfo]:
        """Get NDC info from DailyMed with improved variant matching"""
        try:
            # Use the improved variant generation
            ndc_variants = self.normalize_ndc_for_matching(ndc)
            
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
                            product_name = spl_data.get('title', 'Unknown')
                            
                            # Try to get labeler from API first
                            labeler_name = spl_data.get('labeler', 'Unknown')
                            
                            # If labeler is missing or generic, extract from product name
                            if not labeler_name or labeler_name == 'Unknown' or labeler_name == '':
                                labeler_name = self.extract_labeler_from_product_name(product_name)
                            
                            return ProductInfo(
                                ndc=ndc,
                                product_name=product_name,
                                labeler_name=labeler_name,
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
        """ENHANCED: Find FEI and DUNS numbers in SPL with better pattern matching"""
        matches = []
        
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return matches

            content = response.text
            
            # Multiple patterns to catch different XML structures
            patterns = [
                r'<id\s+([^>]*extension="(\d{7,15})"[^>]*)',  # Standard pattern
                r'extension="(\d{7,15})"',  # Simple extension pattern
                r'root="[^"]*"\s+extension="(\d{7,15})"',  # Root + extension
                r'<id[^>]+extension="(\d{7,15})"[^>]*>',  # ID with extension
            ]
            
            found_numbers = set()
            
            for pattern in patterns:
                if 'extension="(\d{7,15})"' in pattern and pattern.count('(') == 1:
                    # Simple pattern with one capture group
                    id_matches = re.findall(pattern, content, re.IGNORECASE)
                    for extension in id_matches:
                        if isinstance(extension, str):
                            found_numbers.add(extension)
                else:
                    # Complex pattern with multiple capture groups
                    id_matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in id_matches:
                        if isinstance(match, tuple):
                            extension = match[-1]  # Last capture group should be the number
                        else:
                            extension = match
                        found_numbers.add(extension)
            
            # Also look for FEI numbers in text content
            fei_text_pattern = r'\bFEI[:\s#]*(\d{7,15})\b'
            fei_text_matches = re.findall(fei_text_pattern, content, re.IGNORECASE)
            found_numbers.update(fei_text_matches)
            
            # Process all found numbers
            for extension in found_numbers:
                if not extension or len(extension) < 7:
                    continue
                    
                clean_extension = re.sub(r'[^\d]', '', extension)
                
                # Check FEI database first
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
                
                # Check DUNS database if no FEI match
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

    def fallback_establishment_search(self, content: str, target_ndc: str) -> List[Dict]:
        """Fallback method to find establishments by company name matching"""
        establishments_info = []
        
        try:
            # Extract organization names from XML
            org_patterns = [
                r'<name[^>]*>([^<]+)</name>',
                r'<organizationName[^>]*>([^<]+)</organizationName>',
                r'<assignedEntity[^>]*>.*?<name[^>]*>([^<]+)</name>.*?</assignedEntity>',
            ]
            
            found_names = set()
            for pattern in org_patterns:
                matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    clean_name = match.strip()
                    if len(clean_name) > 3 and clean_name not in ['Unknown', 'N/A', 'None']:
                        found_names.add(clean_name)
            
            # Try to match names against database
            for name in found_names:
                name_lower = name.lower()
                
                # Search in FEI database
                for fei_key, establishment in self.fei_database.items():
                    est_name = establishment.get('establishment_name', '').lower()
                    firm_name = establishment.get('firm_name', '').lower()
                    
                    if (name_lower in est_name or est_name in name_lower or 
                        name_lower in firm_name or firm_name in name_lower):
                        
                        establishment_copy = establishment.copy()
                        establishment_copy['fei_number'] = fei_key
                        establishment_copy['operations'] = ['Manufacturing (name match)']
                        establishment_copy['quotes'] = [f'Establishment matched by name: {name}']
                        establishment_copy['match_type'] = 'NAME_MATCH'
                        establishment_copy['xml_location'] = 'SPL Document'
                        
                        establishments_info.append(establishment_copy)
                        break  # Only take first match per name
            
            return establishments_info[:5]  # Limit to 5 matches
            
        except Exception as e:
            return []

    def extract_establishments_with_fei(self, spl_id: str, target_ndc: str) -> Tuple[List[str], List[str], List[Dict]]:
        """ENHANCED: Extract establishments with better fallback mechanisms"""
        try:
            spl_url = f"{self.dailymed_base_url}/services/v2/spls/{spl_id}.xml"
            response = self.session.get(spl_url)

            if response.status_code != 200:
                return [], [], []

            content = response.text
            establishments_info = []
            processed_numbers = set()

            # Primary method: Find FEI/DUNS matches
            matches = self.find_fei_duns_matches_in_spl(spl_id)
            
            if matches:
                establishment_sections = re.findall(r'<assignedEntity[^>]*>.*?</assignedEntity>', content, re.DOTALL | re.IGNORECASE)
                
                for match in matches:
                    if match.fei_number in processed_numbers:
                        continue
                    
                    processed_numbers.add(match.fei_number)
                    
                    if match.match_type == 'FEI_NUMBER':
                        establishment_info = self.lookup_fei_establishment(match.fei_number)
                    else:
                        establishment_info = self.lookup_duns_establishment(match.fei_number)
                    
                    if establishment_info:
                        # Find operations for this establishment
                        establishment_operations = []
                        establishment_quotes = []
                        establishment_included = False
                        
                        # Look in establishment sections
                        for section in establishment_sections:
                            if match.fei_number in section or any(variant in section for variant in self._generate_all_id_variants(match.fei_number)[:5]):
                                name_match = re.search(r'<name[^>]*>([^<]+)</name>', section)
                                section_establishment_name = name_match.group(1) if name_match else establishment_info.get('establishment_name', 'Unknown')
                                
                                ops, quotes = self.extract_ndc_specific_operations(section, target_ndc, section_establishment_name)
                                
                                if ops:
                                    establishment_operations.extend(ops)
                                    establishment_quotes.extend(quotes)
                                    establishment_included = True
                                else:
                                    # Look for general operations
                                    general_ops, general_quotes = self.extract_general_operations(section, section_establishment_name)
                                    if general_ops:
                                        establishment_operations.extend(general_ops)
                                        establishment_quotes.extend([f"General operation: {q}" for q in general_quotes])
                                        establishment_included = True
                                break
                        
                        # If no operations found in sections, look globally in document
                        if not establishment_included:
                            global_ops, global_quotes = self.extract_general_operations(content, establishment_info.get('establishment_name', 'Unknown'))
                            if global_ops:
                                establishment_operations.extend(global_ops)
                                establishment_quotes.extend([f"Document-level operation: {q}" for q in global_quotes])
                                establishment_included = True
                            else:
                                # Include establishment anyway with note
                                establishment_operations = ['Manufacturing (inferred)']
                                establishment_quotes = ['Establishment found in SPL but specific operations not detailed']
                                establishment_included = True
                        
                        if establishment_included:
                            establishment_info['xml_location'] = match.xml_location
                            establishment_info['match_type'] = match.match_type
                            establishment_info['xml_context'] = match.xml_context if hasattr(match, 'xml_context') else ''
                            
                            establishment_operations = list(dict.fromkeys(establishment_operations))
                            establishment_quotes = list(dict.fromkeys(establishment_quotes))
                            
                            establishment_info['operations'] = establishment_operations
                            establishment_info['quotes'] = establishment_quotes
                            
                            establishments_info.append(establishment_info)
            
            # Fallback method: Look for company names in database
            if not establishments_info:
                establishments_info = self.fallback_establishment_search(content, target_ndc)

            return [], [], establishments_info

        except Exception as e:
            return [], [], []

    def extract_ndc_specific_operations(self, section: str, target_ndc: str, establishment_name: str) -> Tuple[List[str], List[str]]:
        """Extract operations - enhanced to find more operations"""
        operations = []
        quotes = []

        ndc_variants = self.normalize_ndc_for_matching(target_ndc)

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

        performance_elements = re.findall(r'<performance[^>]*>.*?</performance>', section, re.DOTALL | re.IGNORECASE)
        business_elements = re.findall(r'<businessOperation[^>]*>.*?</businessOperation>', section, re.DOTALL | re.IGNORECASE)
        
        all_elements = performance_elements + business_elements

        for element in all_elements:
            operation_found = None
            operation_code_match = re.search(r'<code[^>]*code="([^"]*)"[^>]*displayName="([^"]*)"', element, re.IGNORECASE)
            
            if operation_code_match:
                operation_code = operation_code_match.group(1)
                if operation_code in operation_codes:
                    operation_found = operation_codes[operation_code]

            if not operation_found:
                display_name_match = re.search(r'displayName="([^"]*)"', element, re.IGNORECASE)
                if display_name_match:
                    display_name = display_name_match.group(1).lower()
                    if 'manufacture' in display_name and 'api' not in display_name:
                        operation_found = 'Manufacture'
                    elif 'api' in display_name and 'manufacture' in display_name:
                        operation_found = 'API Manufacture'
                    elif 'analysis' in display_name or 'test' in display_name:
                        operation_found = 'Analysis'
                    elif 'pack' in display_name:
                        operation_found = 'Pack'
                    elif 'label' in display_name:
                        operation_found = 'Label'
                    elif 'steriliz' in display_name:
                        operation_found = 'Sterilize'

            if operation_found:
                if '<businessOperation' in element:
                    if operation_found not in operations:
                        operations.append(operation_found)
                        quotes.append(f'Found {operation_found} operation in {establishment_name}')
                else:
                    ndc_code_pattern = r'<code[^>]*code="([^"]*)"[^>]*codeSystem="2\.16\.840\.1\.113883\.6\.69"'
                    ndc_matches = re.findall(ndc_code_pattern, element, re.IGNORECASE)
                    
                    ndc_found_in_operation = False
                    for ndc_code in ndc_matches:
                        clean_ndc = ndc_code.strip()
                        potential_variants = self.normalize_ndc_for_matching(clean_ndc)
                        matching_variants = [v for v in potential_variants if v in ndc_variants]
                        if matching_variants:
                            ndc_found_in_operation = True
                            break

                    if ndc_found_in_operation and operation_found not in operations:
                        operations.append(operation_found)
                        quotes.append(f'Found {operation_found} operation for NDC {target_ndc} in {establishment_name}')

        if 'API Manufacture' in operations and 'Manufacture' in operations:
            operations.remove('Manufacture')

        return operations, quotes

    def extract_general_operations(self, section: str, establishment_name: str) -> Tuple[List[str], List[str]]:
        """Extract general operations from an establishment section"""
        operations = []
        quotes = []

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

        business_operations = re.findall(r'<businessOperation[^>]*>.*?</businessOperation>', section, re.DOTALL | re.IGNORECASE)

        for bus_op in business_operations:
            operation_found = None

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

            if not operation_found:
                for code, operation in operation_codes.items():
                    if code in bus_op:
                        operation_found = operation
                        break

            if operation_found and operation_found not in operations:
                operations.append(operation_found)
                quotes.append(f'Found {operation_found} operation in {establishment_name}')

        if 'API Manufacture' in operations and 'Manufacture' in operations:
            operations.remove('Manufacture')
            quotes = [q for q in quotes if 'Manufacture operation' not in q or 'API Manufacture operation' in q]

        return operations, quotes

    def process_single_ndc(self, ndc: str) -> pd.DataFrame:
        """Process a single NDC number with improved matching and deduplication"""
        if not self.validate_ndc_format(ndc):
            return pd.DataFrame()

        normalized_ndc = self.normalize_ndc(ndc)
        product_info = self.get_ndc_info_comprehensive(normalized_ndc)
        
        if not product_info:
            return pd.DataFrame()

        _, _, establishments_info = self.extract_establishments_with_fei(product_info.spl_id, ndc)

        results = []
        processed_ids = set()

        if establishments_info:
            for establishment in establishments_info:
                unique_id = establishment.get('fei_number') or establishment.get('duns_number')
                
                if unique_id and unique_id in processed_ids:
                    continue
                
                if unique_id:
                    processed_ids.add(unique_id)

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
                    'spl_operations': ', '.join(establishment.get('operations', [])) if establishment.get('operations') else 'None found for this NDC',
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
    st.markdown("Enter an NDC number to discover manufacturing establishments, locations, and operations using FDA data.")
    
    # Auto-load database and show status
    if 'mapper' not in st.session_state:
        with st.spinner("🔄 Loading FDA establishment database..."):
            st.session_state.mapper = NDCToLocationMapper()
            
        if st.session_state.mapper.database_loaded:
            st.success(f"✅ Database loaded successfully!")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("FEI Database Entries", f"{len(st.session_state.mapper.fei_database):,}")
            with col2:
                st.metric("DUNS Database Entries", f"{len(st.session_state.mapper.duns_database):,}")
            with col3:
                current_date = datetime.now().strftime("%Y-%m-%d")
                st.metric("Database Updated", current_date)
                
        else:
            st.error("❌ Could not load establishment database")
            st.info("💡 Please ensure the database file is available in the repository or check GitHub connectivity")
            st.stop()
    
    # NDC input section
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        ndc_input = st.text_input(
            "Enter NDC Number:", 
            placeholder="0185-0674-01",
            help="NDC format: 12345-678-90 or 1234567890"
        )
    with col2:
        search_btn = st.button("🔍 Search", type="primary")
    
    # Example NDCs - include the problematic one
    st.markdown("**Try these examples:**")
    examples = ["0185-0674-01", "50242-061-01", "63323-262-06", "63323-459-14"]
    cols = st.columns(len(examples))
    for i, ex in enumerate(examples):
        with cols[i]:
            if st.button(f"`{ex}`", key=f"ex_{i}"):
                ndc_input = ex
                search_btn = True
    
    # Search functionality
    if search_btn and ndc_input:
        with st.spinner(f"Looking up manufacturing locations for {ndc_input}..."):
            try:
                results_df = st.session_state.mapper.process_single_ndc(ndc_input)
                
                if len(results_df) > 0:
                    first_row = results_df.iloc[0]
                    
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
                        
                        st.info("💡 This product may not have detailed establishment information in its SPL document, or the establishments may not be in the database.")
                    
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
                                    if row['spl_operations'] and row['spl_operations'] != 'None found for this NDC':
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
    
    **Enhanced Features:**
    - ✅ Automatic database loading
    - ✅ Improved NDC format matching
    - ✅ Handles all NDC formats (4-4-2, 5-3-2, 5-4-2)
    - ✅ Enhanced labeler extraction
    - ✅ Real-time establishment analysis
    - ✅ Fallback name matching
    """)
    
    if 'mapper' in st.session_state and st.session_state.mapper.database_loaded:
        st.sidebar.markdown("---")
        st.sidebar.metric("FEI Database Entries", f"{len(st.session_state.mapper.fei_database):,}")
        st.sidebar.metric("DUNS Database Entries", f"{len(st.session_state.mapper.duns_database):,}")
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Database Status:**")
        st.sidebar.success("✅ Loaded and Ready")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Disclaimer:** For informational purposes only.")

if __name__ == "__main__":
    main()
