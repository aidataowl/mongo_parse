#!/usr/bin/env -vS python 

import argparse
import csv
import re
import sys
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# Regular expressions for parsing
CLUSTER_REGEXES: Dict[str, str] = {
    'replica_set_name': r'setName:\s*\'([^\']+)\'',
    'hosts': r'hosts:\s*\[\s*([^\]]*)\s*\]',
    'primary_host': r'primary:\s*\'([^\']+)\''
}

DATABASE_REGEXES: Dict[str, str] = {
    'database_name': r'\*\* DATABASE:\s*([^\s]+)'
}

class Parser:
    """Parser for cluster and database information from log files."""
    
    def __init__(self, config: Dict[str, Dict[str, str]]):
        """
        Initialize the parser with configuration parameters.
        
        Args:
            config: Dictionary containing parsing configuration with regex patterns
        """
        self.config: Dict[str, Dict[str, str]] = config
        self.parsed_data: List[Dict[str, str]] = []

    def read_text_file(self, file_path: str) -> str:
        """
        Read the content of a text file.
        
        Args:
            file_path: Path to the text file
            
        Returns:
            Content of the file as string
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            IOError: If there's an error reading the file
        """
        try:
            with Path(file_path).open('r', encoding='utf-8') as file:
                return file.read()
        except FileNotFoundError:
            logger.error(f"File '{file_path}' not found")
            raise
        except IOError as e:
            logger.error(f"Error reading file '{file_path}': {e}")
            raise

    def parse_cluster_info(self, content: str) -> List[Dict[str, str]]:
        """
        Parse cluster information from content.
        
        Args:
            content: Text content to parse
            
        Returns:
            List of dictionaries containing parsed cluster data
        """
        parsed_records: List[Dict[str, str]] = []
        record: Dict[str, str] = {}

        for field_name, pattern in self.config['cluster'].items():
            match = re.search(pattern, content)
            if match:
                if field_name == 'hosts':
                    hosts = [
                        host.strip().strip("'")
                        for host in match.group(1).split(',')
                        if host.strip()
                    ]
                    record[field_name] = ' | '.join(hosts)
                else:
                    record[field_name] = match.group(1).strip()

        if record:
            parsed_records.append(record)

        return parsed_records

    def parse_database_info(self, content: str) -> List[Dict[str, str]]:
        """
        Parse database names from content.
        
        Args:
            content: Text content to parse
            
        Returns:
            List of dictionaries containing parsed database data
        """
        parsed_records: List[Dict[str, str]] = []
        
        for line_num, line in enumerate(content.strip().split('\n'), 1):
            record: Dict[str, str] = {'line_number': str(line_num)}
            
            for field_name, pattern in self.config['database'].items():
                match = re.search(pattern, line)
                record[field_name] = match.group(1).strip() if match else ''
            
            if record.get('database_name'):
                parsed_records.append(record)
        
        return parsed_records

    def parse_file(self, file_path: str, parse_type: str) -> List[Dict[str, str]]:
        """
        Parse the text file based on the specified parse type.
        
        Args:
            file_path: Path to the text file to parse
            parse_type: Type of parsing ('cluster' or 'database')
            
        Returns:
            List of parsed records
            
        Raises:
            ValueError: If parse_type is invalid
        """
        content = self.read_text_file(file_path)
        
        if parse_type == 'cluster':
            self.parsed_data = self.parse_cluster_info(content)
        elif parse_type == 'database':
            self.parsed_data = self.parse_database_info(content)
        else:
            logger.error(f"Invalid parse type: {parse_type}")
            raise ValueError(f"Invalid parse type: {parse_type}")
        
        return self.parsed_data

    def write_to_csv(self, output_file: Optional[str] = None) -> None:
        """
        Write parsed data to CSV file or stdout.
        
        Args:
            output_file: Path for the output CSV file, or None for stdout
            
        Raises:
            IOError: If there's an error writing to the output
        """
        if not self.parsed_data:
            logger.warning("No data to write to CSV")
            return
        
        fieldnames: List[str] = sorted(set().union(*(record.keys() for record in self.parsed_data)))
        
        try:
            writer = csv.DictWriter(
                sys.stdout if output_file is None else open(output_file, 'w', newline='', encoding='utf-8'),
                fieldnames=fieldnames
            )
            writer.writeheader()
            writer.writerows(self.parsed_data)
            
            logger.info(f"Successfully wrote {len(self.parsed_data)} records")
            
        except IOError as e:
            logger.error(f"Error writing to CSV: {e}")
            raise
        finally:
            if output_file is not None:
                writer._dict_to_list(None).close()  # type: ignore

def parse_arguments() -> Tuple[str, str]:
    """
    Parse command-line arguments.
    
    Returns:
        Tuple of parse type and input file path
    
    Raises:
        SystemExit: If arguments are invalid
    """
    parser = argparse.ArgumentParser(
        description="Parse MongoDB cluster or database information from log files",
        epilog="Output is written to stdout and can be redirected to a file"
    )
    parser.add_argument(
        'parse_type',
        choices=['cluster', 'database'],
        help="Type of parsing to perform: 'cluster' or 'database'"
    )
    parser.add_argument(
        'input_file',
        type=str,
        help="Path to the input log file"
    )
    
    args = parser.parse_args()
    
    if not Path(args.input_file).is_file():
        logger.error(f"Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    return args.parse_type, args.input_file

def main() -> None:
    """
    Main function to parse MongoDB information from log files.
    
    Raises:
        SystemExit: If an error occurs during processing
    """
    try:
        parse_type, input_file = parse_arguments()
        
        config = {
            'cluster': CLUSTER_REGEXES,
            'database': DATABASE_REGEXES
        }
        
        logger.info(f"Parsing {parse_type} information from: {input_file}")
        
        parser = Parser(config)
        parser.parse_file(input_file, parse_type)
        parser.write_to_csv()
        
        logger.info("Parsing complete")
        logger.info(f"Records processed: {len(parser.parsed_data)}")
        
        if parser.parsed_data:
            logger.info("First record preview:")
            for key, value in parser.parsed_data[0].items():
                logger.info(f"  {key}: {value}")
                
    except (ValueError, IOError) as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()