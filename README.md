# Maschinensucher Web Scraper

A professional web scraping tool for extracting dealer and machine data from maschinensucher.de, built with Python and crawl4ai. This tool provides comprehensive data extraction capabilities with robust error handling, retry mechanisms, and output in multiple formats.

## Features

### 🔍 **Comprehensive Data Extraction**
- **Dealer Information**: Company names, addresses, contact details, and geographic location
- **Machine Inventory**: Categories, subcategories, and inventory counts per dealer
- **Contact Details**: Phone numbers, fax, and contact persons (with phone number validation)
- **Address Parsing**: German address component extraction with structured fields

### 🚀 **Performance & Reliability**
- **Parallel Processing**: Concurrent crawling for faster data extraction
- **Smart Retry Logic**: Exponential backoff with configurable retry parameters
- **Rate Limiting**: Configurable delays to respect website policies
- **Error Handling**: Comprehensive logging and screenshot capture on failures
- **Memory Management**: Adaptive concurrency control based on system resources

### 📊 **Flexible Output Options**
- **CSV Export**: Flattened data structure with category analytics
- **JSON Export**: Hierarchical data structure preserving original format
- **Data Validation**: Phone number validation and data cleaning
- **Timestamped Files**: Automatic file naming with category and timestamp

### 🛠️ **Advanced Configuration**
- **Schema-Based Extraction**: Dynamic CSS/JSON extraction schemas
- **Category Filtering**: Support for different machine categories
- **Pagination Support**: Multi-page crawling capabilities
- **Debug Features**: Skip lists, verbose logging, and error screenshots

## Installation

### Prerequisites
- Python 3.8+
- Required dependencies listed in `requirements.txt`

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd maschinensucher

# Install dependencies
pip install -r requirements.txt

# Create necessary directories
mkdir -p output schemas browser_data
```

## Usage

### Basic Usage
```bash
# Scrape dealers from default category (Holzbearbeitungsmaschinen)
python maschinensucher.py

# Scrape from specific category with multiple pages
python maschinensucher.py --category 2 --pages 5

# Include contact information extraction
python maschinensucher.py --category 3 --pages 2 --contact
```

### Command Line Options
```bash
python maschinensucher.py [OPTIONS]

Options:
  --category TEXT     Category code (tci-<category> from URL)
                     Default: "3" (Holzbearbeitungsmaschinen)
  --pages INTEGER     Number of pages to scrape (default: 1)
  --contact          Extract detailed contact information
  --force-schema     Force regeneration of extraction schemas
  --help             Show help message
```

### Category Codes
Common category codes for maschinensucher.de:
- `2`: Metallbearbeitung (Metal Processing)
- `3`: Holzbearbeitungsmaschinen (Woodworking Machines)
- `16`: Druckmaschinen (Printing Machines)
- `19`: Lebensmitteltechnik (Food Technology)

## Project Structure

```
maschinensucher/
├── maschinensucher.py          # Main application
├── helpers.py                  # Utility functions
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── schemas/                    # Extraction schemas
│   ├── dealer_schema.json      # Dealer list extraction
│   ├── dealer_info.json        # Dealer contact details
│   └── machines_schema.json    # Machine inventory data
├── output/                     # Generated files
│   ├── *.csv                   # CSV exports
│   ├── *.json                  # JSON exports
│   └── error_screenshots/      # Debug screenshots
├── website_template/           # HTML templates for testing
└── __pycache__/               # Python cache files
```

## Core Functionality

### 1. Dealer List Extraction (`grab_dealer_list`)
Extracts dealer listings with:
- Company names and addresses
- Distance from location
- Category information
- Dealer profile links

### 2. Contact Information (`grab_dealer_info_sequential`)
Detailed contact extraction with:
- Phone number validation
- Contact person names
- Fax numbers
- Retry logic for failed extractions

### 3. Machine Inventory (`grab_dealer_machines_parallel`)
Parallel extraction of machine data:
- Main categories and counts
- Subcategories with inventory
- Dealer-specific machine listings

### 4. Data Processing & Export
- Address component parsing
- Category analytics generation
- Data validation and cleaning
- Multi-format output generation

## Configuration

### Crawling Strategy Options
The application supports different crawling strategies with configurable parameters:

#### Conservative (Recommended)
```python
delay_between_requests = 8.0     # 7-8 requests/minute
max_retries = 2
retry_base_delay = 10.0
retry_max_delay = 60.0
```

#### Balanced
```python
delay_between_requests = 6.0     # 10 requests/minute
max_retries = 3
retry_base_delay = 8.0
retry_max_delay = 45.0
```

#### Aggressive (Use with caution)
```python
delay_between_requests = 4.0     # 15 requests/minute
max_retries = 2
retry_base_delay = 6.0
retry_max_delay = 30.0
```

### Schema Configuration
Extraction schemas are defined in JSON files and can be automatically generated or manually configured:
- `dealer_schema.json`: Dealer listing extraction
- `dealer_info.json`: Contact detail extraction
- `machines_schema.json`: Machine inventory extraction

## Output Format

### CSV Output
Flattened structure with columns:
- Dealer information (company_name, address components, contact details)
- Machine categories (main_category, main_category_count)
- Subcategories (sub_category_1, sub_category_1_count, etc.)
- Metadata (source_url, page_number, timestamp)

### JSON Output
Hierarchical structure preserving:
- Original data relationships
- Nested subcategory arrays
- Complete metadata

## Error Handling & Debugging

### Logging
- Configurable log levels (INFO, DEBUG, WARNING, ERROR)
- File logging (`app.log`) and console output
- Detailed extraction progress tracking

### Error Recovery
- Screenshot capture on extraction failures
- Exponential backoff retry mechanism
- Graceful handling of network timeouts
- Data consistency validation

### Debug Features
- Skip lists for problematic dealers
- Verbose extraction logging
- Error screenshot storage
- Performance timing metrics

## Dependencies

Key dependencies include:
- `crawl4ai`: Advanced web crawling framework
- `pandas`: Data manipulation and analysis
- `asyncio`: Asynchronous programming support
- `beautifulsoup4`: HTML parsing
- `aiohttp`: Async HTTP client

See `requirements.txt` for complete dependency list.

## Performance Considerations

### System Requirements
- Minimum 4GB RAM recommended
- SSD storage for better I/O performance
- Stable internet connection

### Optimization Tips
- Use parallel processing for large datasets
- Adjust retry parameters based on network conditions
- Monitor memory usage for large crawling operations
- Use appropriate delays to avoid rate limiting

## Legal & Ethical Considerations

⚠️ **Important Notice**: This tool is designed for legitimate research and business purposes. Users must:
- Respect website terms of service
- Implement appropriate rate limiting
- Avoid overloading target servers
- Comply with applicable data protection laws
- Use extracted data responsibly

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with appropriate tests
4. Submit a pull request with detailed description

## License

This project is provided as-is for educational and research purposes. Users are responsible for ensuring compliance with applicable laws and website terms of service.

## Support

For issues, questions, or contributions:
- Check the existing documentation
- Review log files for error details
- Examine schema files for extraction logic
- Test with single pages before bulk operations

---

*Last updated: May 30, 2025*
