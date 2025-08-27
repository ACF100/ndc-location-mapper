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
            
            # If no local file found, try downloading from GitHub
            github_urls = [
                "https://raw.githubusercontent.com/yourusername/ndc-location-mapper/main/drls_reg.xlsx",
                "https://github.com/yourusername/ndc-location-mapper/raw/main/drls_reg.xlsx"
            ]
            
            for github_url in github_urls:
                try:
                    st.info("🌐 Downloading establishment database from GitHub...")
                    response = requests.get(github_url, timeout=30)
                    if response.status_code == 200:
                        temp_file = "temp_drls_reg.xlsx"
                        with open(temp_file, "wb") as f:
                            f.write(response.content)
                        
                        self.load_fei_database_from_spreadsheet(temp_file)
                        os.remove(temp_file)  # Clean up
                        
                        if self.fei_database or self.duns_database:
                            self.database_loaded = True
                            return
                            
                except Exception as e:
                    continue
            
            # If all else fails, show error
            st.error("❌ Could not load establishment database from any source")
            st.info("💡 Please ensure drls_reg.xlsx is available in the repository")
            
        except Exception as e:
            st.error(f"❌ Error during database loading: {str(e)}")

    # [Include all the previous methods here - load_fei_database_from_spreadsheet, _generate_all_id_variants, etc.]
    # Then add the enhanced methods:

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
            st.error(f"Error finding matches: {str(e)}")
            
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
            st.error(f"Error extracting establishments: {str(e)}")
            return [], [], []

    # [Include all other existing methods from the original code]

def main():
    # [Include the existing main() function]
    pass

if __name__ == "__main__":
    main()
