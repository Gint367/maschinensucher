import argparse
import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    CrawlResult,
    JsonCssExtractionStrategy,
    LLMConfig,
    MemoryAdaptiveDispatcher
)
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy  # Add this import
from helpers import (
    is_valid_phone_number,
    exponential_backoff_delay,
    extract_dealer_id_from_link,
)

browser_config = BrowserConfig(headless=True, verbose=True, text_mode=True)


def load_schemas_from_json(
    schema_file_path: str = os.path.join("schemas", "dealer_schema.json"),
) -> List[Dict[str, Any]]:
    """
    Load extraction schemas from JSON file.
    This function is now primarily used internally by build_schema_dealer.

    Args:
        schema_file_path (str): Path to the schema JSON file

    Returns:
        List[Dict[str, Any]]: List of schema dictionaries

    Raises:
        FileNotFoundError: If the schema file doesn't exist
        json.JSONDecodeError: If the schema file contains invalid JSON
    """
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema file not found: {schema_file_path}")

    try:
        with open(schema_file_path, "r", encoding="utf-8") as f:
            schemas = json.load(f)

        if not isinstance(schemas, list):
            raise ValueError("Schema file must contain a list of schema objects")

        logging.debug(f"Loaded {len(schemas)} schemas from {schema_file_path}")
        return schemas

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in schema file {schema_file_path}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error loading schemas from {schema_file_path}: {str(e)}")
        raise


async def grab_dealer_list(
    num_pages: int = 1, category_code: str = "2"
) -> List[Dict[str, Any]]:
    """
    Grab dealer list from maschinensucher.de with pagination support.

    Args:
        num_pages (int): Number of pages to crawl, defaults to 1
        category_code (str): Category code for the dealer search, defaults to "2"

    Returns:
        List[Dict[str, Any]]: List of dealer dictionaries containing extracted data
    """
    logging.info(f"Searching for dealers across {num_pages} pages...")

    all_dealers: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                keep_attrs=["id", "class"],
                keep_data_attributes=True,
                delay_before_return_html=1.0,
                wait_for="css:body",  # Wait for page to load
                scraping_strategy=LXMLWebScrapingStrategy()
            )

            for page in range(1, num_pages + 1):
                url = f"https://www.maschinensucher.de/Haendler/tci-{category_code}?page={page}&sort=kilometer"
                logging.info(f"Crawling page {page}: {url}")

                result = await crawler.arun(
                    url,
                    config=crawl_config,
                )  # type: ignore

                if result.success:  # type: ignore
                    # Get the latest schemas using build_schema_dealer
                    schemas = await build_schema_dealer(
                        cleaned_html=result.html, force=False
                    )  # type: ignore

                    # Track category text and extracted dealer data for this page
                    category_text = ""
                    page_dealers_count = 0

                    # First pass: Extract all data from schemas
                    extracted_data_by_schema = {}
                    for schema in schemas:
                        schema_name = schema.get("name", "Unknown Schema")
                        logging.debug(f"Processing schema: {schema_name}")

                        extracted_data = JsonCssExtractionStrategy(
                            schema=schema,
                        ).run(url=url, sections=[result.html])  # type: ignore

                        extracted_data_by_schema[schema_name] = extracted_data
                        logging.debug(f"Extracted data from {schema_name} schema")

                    # Second pass: Process extracted data in the correct order
                    # First, extract category text
                    if "Category Text" in extracted_data_by_schema:
                        category_data = extracted_data_by_schema["Category Text"]
                        if isinstance(category_data, list) and len(category_data) > 0:
                            if (
                                isinstance(category_data[0], dict)
                                and "text" in category_data[0]
                            ):
                                category_text = category_data[0]["text"]
                        logging.debug(f"Extracted category text: {category_text}")

                    # Then, process dealer data with the correct category text
                    if "Maschinensucher Dealer Card" in extracted_data_by_schema:
                        dealer_data = extracted_data_by_schema[
                            "Maschinensucher Dealer Card"
                        ]
                        if isinstance(dealer_data, list):
                            for dealer in dealer_data:
                                if isinstance(dealer, dict):
                                    
                                    dealer["page_number"] = page
                                    dealer["source_url"] = url
                                    dealer["category"] = category_text
                                    dealer["category_id"] = category_code

                                    # Enrich with parsed address data
                                    dealer = enrich_dealer_with_address_data(dealer)

                            all_dealers.extend(dealer_data)
                            page_dealers_count += len(dealer_data)
                            logging.debug(
                                f"Extracted {len(dealer_data)} dealers with category: '{category_text}'"
                            )

                    # Handle other schema types
                    for schema_name, extracted_data in extracted_data_by_schema.items():
                        if schema_name not in [
                            "Category Text",
                            "Maschinensucher Dealer Card",
                        ]:
                            logging.debug(
                                f"Processing {schema_name} schema - no specific handler implemented yet"
                            )
                            if isinstance(extracted_data, list):
                                logging.debug(
                                    f"Extracted {len(extracted_data)} items from {schema_name} schema"
                                )

                    logging.info(
                        f"Extracted {page_dealers_count} dealers from page {page}"
                    )
                else:
                    logging.error(
                        f"Failed to crawl page {page}: {result.error_message}"
                    )  # type: ignore

            logging.info(f"Total dealers extracted: {len(all_dealers)}")

        except Exception as e:
            logging.error(f"Error during crawling: {str(e)}")
            raise

    return all_dealers

async def grab_dealer_info_parallel( # TODO: NOT WORKING YET, use se sequential version for now
    dealer_links 
) -> pd.DataFrame:
    """
    Grab detailed dealer information from maschinensucher.de using provided dealer links.
    Supports both single link (str) and multiple links (List[str]).
    Extracts contact details including phone, fax, and contact person.
    
    Args:
        dealer_links: Single dealer link (str) or list of dealer links (List[str])
        
    Returns:
        pd.DataFrame: DataFrame with columns: contact_person, phone_number, fax_number, id
    """
    # Normalize input to always be a list
    if isinstance(dealer_links, str):
        dealer_links = [dealer_links]
    elif not isinstance(dealer_links, list):
        raise ValueError("dealer_links must be a string or list of strings")
    
    logging.info(f"Grabbing detailed dealer information for {len(dealer_links)} dealers...")
    
    js_code_to_show_phones = """
        task = async () => {
            document.querySelector('#collapse-phone a[data-bs-toggle="collapse"]').click();
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        await task();
    """
    
    all_contact_details = []
    special_browser_config = BrowserConfig(
        use_persistent_context=True, 
        headless=True, 
        verbose=True, 
        text_mode=True,
        user_data_dir=os.path.join("browser_data", "maschinensucher_dealer")
    )
    async with AsyncWebCrawler(config=special_browser_config) as crawler:
        # Convert relative links to absolute URLs if needed
        full_urls = []
        for link in dealer_links:
            if link.startswith('/'):
                full_urls.append(f"https://www.maschinensucher.de{link}")
            else:
                full_urls.append(link)
        
        # Configure crawler
        crawl_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            js_code=js_code_to_show_phones,
            wait_for='css:body',
            delay_before_return_html=2.0,
            keep_attrs=["id", "class"],
            keep_data_attributes=True,
        )
        
        # Load schemas
        schemas = load_schemas_from_json(os.path.join("schemas", "dealer_info.json"))
        
        # Crawl all pages in parallel
        results: List[CrawlResult] = await crawler.arun_many(urls=full_urls, config=crawl_config)
        
        # Process each result
        for i, result in enumerate(results):
            dealer_link = dealer_links[i]  # Use original link for ID extraction
            dealer_id = extract_dealer_id_from_link(dealer_link)
            url = full_urls[i]
            
            if result.success:
                # Use JsonCssExtractionStrategy to extract data
                for schema in schemas:
                    schema_name = schema.get("name", "Unknown Schema")
                    logging.debug(f"Processing schema: {schema_name} for dealer {dealer_id}")
                    
                    # Ensure result.html is not None before passing to extraction
                    current_html_content = result.html if result.html is not None else ""
                    if not current_html_content:
                        logging.warning(f"HTML content is empty for URL: {url}. Skipping schema extraction for {schema_name}.")
                        continue
                    
                    try:
                        extracted_data = JsonCssExtractionStrategy(
                            schema=schema,
                        ).run(url=url, sections=[current_html_content])
                        
                        logging.debug(f"Extracted data from {schema_name} schema (dealer {dealer_id}): {extracted_data}")
                        
                        if isinstance(extracted_data, list):
                            for item in extracted_data:
                                if isinstance(item, dict):
                                    contact_entry = {
                                        'contact_person': item.get('contact_person', ''),
                                        'phone_number': item.get('phone', ''),
                                        'fax_number': item.get('fax', ''),
                                        'id': dealer_id
                                    }
                                    all_contact_details.append(contact_entry)
                        elif isinstance(extracted_data, dict):
                            contact_entry = {
                                'contact_person': extracted_data.get('contact_person', ''),
                                'phone_number': extracted_data.get('phone', ''),
                                'fax_number': extracted_data.get('fax', ''),
                                'id': dealer_id
                            }
                            all_contact_details.append(contact_entry)
                    
                    except Exception as e:
                        logging.error(f"Error extracting data from schema {schema_name} for dealer {dealer_id}: {str(e)}")
                        continue
            else:
                logging.error(f"Failed to extract contact details for dealer {dealer_id}: {result.error_message}")
                # Add empty entry to maintain data consistency
                empty_entry = {
                    'contact_person': '',
                    'phone_number': '',
                    'fax_number': '',
                    'id': dealer_id
                }
                all_contact_details.append(empty_entry)
    
    # Create DataFrame with consistent column structure
    df = pd.DataFrame(all_contact_details)
    
    # Ensure all expected columns exist even if no data was extracted
    if df.empty:
        df = pd.DataFrame(columns=['contact_person', 'phone_number', 'fax_number', 'id'])
    # Print DataFrame contents for debugging
    print("\n=== CONTACT DETAILS DATAFRAME ===")
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print("\nDataFrame contents:")
    print(df.to_string())
    print("\n" + "="*50)
    logging.info(f"Successfully extracted contact details for {len(df)} dealers")
    return df

async def grab_dealer_info_sequential(
    dealer_links,
    delay_between_requests: float = 5.0,
    max_retries: int = 1,
    retry_base_delay: float = 5.0,
    retry_max_delay: float = 30.0
) -> pd.DataFrame:
    """
    Grab detailed dealer information from maschinensucher.de using provided dealer links.
    Supports both single link (str) and multiple links (List[str]).
    Extracts contact details including phone, fax, and contact person.
    
    Features robust retry logic with exponential backoff for failed phone number extractions.
    
    Args:
        dealer_links: Single dealer link (str) or list of dealer links (List[str])
        delay_between_requests (float): Delay in seconds between each dealer request (default: 2.0)
        max_retries (int): Maximum number of retry attempts for failed extractions (default: 3)
        retry_base_delay (float): Base delay for exponential backoff in seconds (default: 2.0)
        retry_max_delay (float): Maximum delay for exponential backoff in seconds (default: 30.0)
        
    Returns:
        pd.DataFrame: DataFrame with columns: contact_person, phone_number, fax_number, id
    """
    # Normalize input to always be a list
    if isinstance(dealer_links, str):
        dealer_links = [dealer_links]
    elif not isinstance(dealer_links, list):
        raise ValueError("dealer_links must be a string or list of strings")
    
    logging.info(f"Grabbing detailed dealer information for {len(dealer_links)} dealers...")
    
    js_code_to_show_phones = """
    (async function() {
        // Function to wait for element to have specific classes
        function waitForElement(selector, condition, timeout) {
            timeout = timeout || 3000;
            return new Promise(function(resolve, reject) {
                var startTime = Date.now();
                
                function checkElement() {
                    var element = document.querySelector(selector);
                    if (element && condition(element)) {
                        resolve(element);
                    } else if (Date.now() - startTime > timeout) {
                        reject(new Error('Timeout waiting for ' + selector));
                    } else {
                        setTimeout(checkElement, 100);
                    }
                }
                
                checkElement();
            });
        }
        
        // First, click the phone toggle button
        var phoneButton = document.querySelector('#collapse-phone a[data-bs-toggle="collapse"]');
        if (phoneButton) {
            phoneButton.click();
            
            // Wait for the phone div to become visible (has 'show' class and is not hidden)
            await waitForElement('#link-phone', function(el) {
                return el.classList.contains('show') && 
                       el.style.display !== 'none' &&
                       el.offsetHeight > 0;
            });
            
            console.log('Phone number revealed and loaded');
        }
        })();
    """     
    all_contact_details = []
    
    # Load schemas once outside the loop
    schemas = load_schemas_from_json(os.path.join("schemas", "dealer_info.json"))
    # Process each dealer link individually with its own crawler instance
    for i, dealer_link in enumerate(dealer_links):
        dealer_id = extract_dealer_id_from_link(dealer_link)
        
        # Convert relative link to absolute URL if needed
        if dealer_link.startswith('/'):
            url = f"https://www.maschinensucher.de{dealer_link}"
        else:
            url = dealer_link
        
        logging.info(f"Processing dealer {i+1}/{len(dealer_links)} - ID: {dealer_id}")
        logging.debug(f"URL: {url}")
        
        # Apply delay between requests (except for the first request)
        if i > 0:
            logging.debug(f"Applying delay of {delay_between_requests}s between requests")
            await asyncio.sleep(delay_between_requests)
        
        # Retry logic for each dealer
        dealer_success = False
        contact_person = ''
        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                # Create a fresh crawler instance for each attempt
                special_browser_config = BrowserConfig(
                    headless=True, 
                    verbose=True,
                    browser_mode="builtin",
                    text_mode=True,
                    
                )
                
                async with AsyncWebCrawler(config=special_browser_config) as crawler:
                    # Configure crawler
                    crawl_config = CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        js_code=js_code_to_show_phones,
                        wait_for='css:#link-phone',
                        delay_before_return_html=2.0,
                        keep_attrs=["id", "class"],
                        keep_data_attributes=True,
                        magic=True,  # Enable magic mode for better JS handling
                    )
                    
                    # Crawl the individual page
                    result = await crawler.arun(url, config=crawl_config)
                    
                    if result.success:
                        extracted_valid_data = False
                        temp_contact_details = []
                        
                        # Use JsonCssExtractionStrategy to extract data
                        for schema in schemas:
                            schema_name = schema.get("name", "Unknown Schema")
                            logging.debug(f"Processing schema: {schema_name} for dealer {dealer_id} (attempt {attempt + 1})")
                            
                            # Ensure result.html is not None before passing to extraction
                            current_html_content = result.html if result.html is not None else ""
                            if not current_html_content:
                                logging.warning(f"HTML content is empty for URL: {url}. Skipping schema extraction for {schema_name}.")
                                continue
                            
                            try:
                                extracted_data = JsonCssExtractionStrategy(
                                    schema=schema,
                                ).run(url=url, sections=[current_html_content])
                                
                                logging.debug(f"Extracted data from {schema_name} schema (dealer {dealer_id}): {extracted_data}")
                                
                                if isinstance(extracted_data, list):
                                    for item in extracted_data:
                                        if isinstance(item, dict):
                                            phone_number = item.get('phone', '')
                                            contact_person = item.get('contact_person', '')
                                            contact_entry = {
                                                'contact_person': contact_person,
                                                'phone_number': phone_number,
                                                'fax_number': item.get('fax', ''),
                                                'id': dealer_id
                                            }
                                            temp_contact_details.append(contact_entry)
                                            
                                            # Check if we got a valid phone number
                                            if is_valid_phone_number(phone_number):
                                                extracted_valid_data = True
                                                
                                elif isinstance(extracted_data, dict):
                                    phone_number = extracted_data.get('phone', '')
                                    contact_person = extracted_data.get('contact_person', '')
                                    contact_entry = {
                                        'contact_person': contact_person,
                                        'phone_number': phone_number,
                                        'fax_number': extracted_data.get('fax', ''),
                                        'id': dealer_id
                                    }
                                    temp_contact_details.append(contact_entry)
                                    
                                    # Check if we got a valid phone number
                                    if is_valid_phone_number(phone_number):
                                        extracted_valid_data = True
                            
                            except Exception as e:
                                logging.error(f"Error extracting data from schema {schema_name} for dealer {dealer_id}: {str(e)}")
                                continue
                        # Check if we extracted valid data
                        if extracted_valid_data:
                            logging.info(f"Successfully extracted valid contact data for dealer {dealer_id} on attempt {attempt + 1}")
                            # Add the extracted data to final results
                            all_contact_details.extend(temp_contact_details)
                            dealer_success = True
                            break
                        elif attempt == max_retries:
                            # Last attempt and still no valid data - don't add invalid data
                            logging.warning(f"No valid phone number found for dealer {dealer_id} after {max_retries + 1} attempts")
                            dealer_success = False  # Will trigger empty entry addition later
                            break
                        else:
                            # Invalid data detected, prepare for retry
                            invalid_phones = [entry['phone_number'] for entry in temp_contact_details if entry['phone_number']]
                            logging.warning(f"Invalid phone number(s) detected for dealer {dealer_id} on attempt {attempt + 1}: {invalid_phones}")
                            logging.info(f"Retrying dealer {dealer_id} in {retry_base_delay * (2 ** attempt):.1f}s...")
                            
                            # Apply exponential backoff before retry
                            if attempt < max_retries:
                                await exponential_backoff_delay(attempt, retry_base_delay, retry_max_delay)
                    else:
                        logging.error(f"Failed to crawl dealer {dealer_id} on attempt {attempt + 1}: {result.error_message}")
                        if attempt < max_retries:
                            logging.info(f"Retrying dealer {dealer_id} in {retry_base_delay * (2 ** attempt):.1f}s...")
                            await exponential_backoff_delay(attempt, retry_base_delay, retry_max_delay)
                        
            except Exception as e:
                logging.error(f"Exception occurred for dealer {dealer_id} on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries:
                    logging.info(f"Retrying dealer {dealer_id} in {retry_base_delay * (2 ** attempt):.1f}s...")
                    await exponential_backoff_delay(attempt, retry_base_delay, retry_max_delay)
        
        # If all attempts failed, leave the phone number empty
        if not dealer_success:
            logging.error(f"All attempts failed for dealer {dealer_id}, leave phone number empty")
            empty_entry = {
                'contact_person': contact_person,
                'phone_number': '',
                'fax_number': '',
                'id': dealer_id
            }
            all_contact_details.append(empty_entry)
    
    # Create DataFrame with consistent column structure
    df = pd.DataFrame(all_contact_details)
    
    # Ensure all expected columns exist even if no data was extracted
    if df.empty:
        df = pd.DataFrame(columns=['contact_person', 'phone_number', 'fax_number', 'id'])
    # Print DataFrame contents for debugging
    logging.debug("\n=== CONTACT DETAILS DATAFRAME ===")
    logging.debug(f"DataFrame shape: {df.shape}")
    logging.debug(f"Columns: {list(df.columns)}")
    logging.info("\nDataFrame contents:")
    logging.info(df.to_string())
    logging.debug("\n" + "="*50)
    logging.info(f"Successfully extracted contact details for {len(df)} dealers")
    return df



async def grab_dealer_machines_parallel(
    dealer_ids: List[str], category_code: str, num_pages: int = 1
) -> pd.DataFrame:
    """
    Parallel version of grab_dealer_machines using crawl4ai's arun_many() method.
    
    Grab machine list for multiple dealers from maschinensucher.de in parallel.
    This function crawls all dealers simultaneously instead of sequentially.
    example output of a machine:
    {
        "category_name": "Holzbearbeitungsmaschinen",
        "count": "5",
        "sub_categories": [
            {
            "category_name": "Zimmereimaschinen",
            "count": "5"
            }
        ],
        "source_url": "https://www.maschinensucher.de/main/search/index?customer-number=46184&main-category-ids[]=3&page=1",
        "dealer_id": "46184",
        "category_id_filter": "3"
    }
    Args:
        dealer_ids (List[str]): List of dealer customer numbers.
        category_code (str): Category code for the machine search.
        num_pages (int): Number of pages to crawl per dealer, defaults to 1.

    Returns:
        pd.DataFrame: DataFrame containing extracted machine data from all dealers.
    """
    logging.info(
        f"Parallel searching for machines for {len(dealer_ids)} dealers in category {category_code} (page {num_pages})..."
    )
    all_machines: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            # Generate all URLs for parallel crawling (one URL per dealer)
            urls = [
                f"https://www.maschinensucher.de/main/search/index?customer-number={dealer_id}&main-category-ids[]={category_code}&page={num_pages}"
                for dealer_id in dealer_ids
            ]
            
            logging.debug(f"Generated {len(urls)} URLs for parallel crawling")
            
            # Configure crawler with conservative rate limiting
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                keep_attrs=["id", "class"],
                keep_data_attributes=True,
                delay_before_return_html=1.0,
                wait_for="css:body",  # Wait for page to load
            )
            
            # Use MemoryAdaptiveDispatcher for intelligent concurrency control
            try:
                dispatcher = MemoryAdaptiveDispatcher(
                    memory_threshold_percent=90.0,
                    check_interval=1,
                )
                logging.debug("Using MemoryAdaptiveDispatcher for concurrency control")
            except ImportError:
                logging.warning("MemoryAdaptiveDispatcher not available, using default dispatcher")
                dispatcher = None
            
            # Crawl all pages in parallel
            start_time = time.time()
            if dispatcher:
                results = await crawler.arun_many(
                    urls=urls,
                    config=crawl_config,
                    dispatcher=dispatcher
                )
            else:
                results = await crawler.arun_many(
                    urls=urls,
                    config=crawl_config
                )
            # Ensure results is a list (not a generator)
            if hasattr(results, '__aiter__'):
                # If it's an async generator, collect all results
                results = [result async for result in results]

            crawl_duration = time.time() - start_time
            logging.info(f"Parallel crawling completed in {crawl_duration:.2f} seconds")
            
            # Pre-generate schemas once for efficiency
            schemas = None
            successful_results = [r for r in results if r.success]  # type: ignore
            
            if successful_results:
                # Use first successful result to generate schemas
                first_html = successful_results[0].html  # type: ignore
                if first_html:
                    schemas = await build_schema_machines(cleaned_html=first_html)
                    logging.debug(f"Generated {len(schemas) if schemas else 0} schemas for extraction")
            
            # Process all results
            total_dealers_processed = 0
            failed_dealers = 0
            
            for i, result in enumerate(results):
                dealer_id = dealer_ids[i]
                url = urls[i]
                
                if result.success:  # type: ignore
                    total_dealers_processed += 1
                    dealer_machines_count = 0
                    
                    # Use pre-generated schemas or generate if needed
                    current_schemas = schemas
                    if not current_schemas and result.html:  # type: ignore
                        current_schemas = await build_schema_machines(cleaned_html=result.html)  # type: ignore
                    
                    if not current_schemas:
                        logging.warning(f"No schemas available for dealer {dealer_id}, skipping extraction")
                        continue
                    
                    for schema in current_schemas:
                        schema_name = schema.get("name", "Unknown Schema")
                        logging.debug(f"Processing schema: {schema_name} for dealer {dealer_id}")

                        # Ensure result.html is not None before passing to extraction
                        current_html_content = (
                            result.html if result.html is not None else ""  # type: ignore
                        )
                        if not current_html_content:
                            logging.warning(
                                f"HTML content is empty for URL: {url}. Skipping schema extraction for {schema_name}."
                            )
                            continue

                        try:
                            extracted_data = JsonCssExtractionStrategy(
                                schema=schema,
                            ).run(url=url, sections=[current_html_content])  # type: ignore

                            # Decode Unicode escape sequences from crawl4ai extraction
                            #extracted_data = decode_unicode_escapes(extracted_data)

                            logging.debug(
                                f"Extracted data from {schema_name} schema (dealer {dealer_id}): {json.dumps(extracted_data, indent=2, ensure_ascii=False) if extracted_data else 'No data'}"
                            )

                            if isinstance(extracted_data, list):
                                for item in extracted_data:
                                    if isinstance(item, dict):
                                        item["source_url"] = url
                                        item["dealer_id"] = dealer_id
                                        item["category_id_filter"] = category_code
                                        item["page_number"] = num_pages  # Current page being processed
                                all_machines.extend(
                                    [d for d in extracted_data if isinstance(d, dict)]
                                )
                                dealer_machines_count += len(extracted_data)
                            elif isinstance(extracted_data, dict):
                                extracted_data["source_url"] = url
                                extracted_data["dealer_id"] = dealer_id
                                extracted_data["category_id_filter"] = category_code
                                extracted_data["page_number"] = num_pages  # Current page being processed
                                all_machines.append(extracted_data)
                                dealer_machines_count += 1
                        
                        except Exception as e:
                            logging.error(f"Error extracting data from schema {schema_name} for dealer {dealer_id}: {str(e)}")
                            continue
                else:
                    failed_dealers += 1
                    logging.error(
                        f"Failed to crawl dealer {dealer_id}: {result.error_message}"  # type: ignore
                    )
            
            # Log summary statistics
            logging.info(
                f"Parallel crawling summary: "
                f"{total_dealers_processed}/{len(dealer_ids)} dealers successful, "
                f"{failed_dealers} failed, "
                f"{len(all_machines)} total machines extracted"
            )
            
        except Exception as e:
            logging.error(
                f"Error during parallel machine crawling for dealers {dealer_ids}: {str(e)}"
            )
            # Fallback to sequential crawling on error
            logging.warning("Falling back to sequential crawling for dealers")

    # Convert to DataFrame
    if all_machines:
        machines_df = pd.DataFrame(all_machines)
        logging.info(f"Created DataFrame with {len(machines_df)} machine records")
        return machines_df
    else:
        logging.warning("No machine data found, returning empty DataFrame")
        return pd.DataFrame()



def clean_html_address(raw_html: str) -> str:
    """
    Clean raw HTML address string by removing HTML tags and extra whitespace.

    Args:
        raw_html (str): Raw HTML string from address extraction

    Returns:
        str: Cleaned HTML string with normalized whitespace and removed HTML tags
    """
    if not raw_html:
        logging.warning("Empty raw HTML address provided")
        return ""

    # Remove HTML tags (like <span> and </span>)
    cleaned = re.sub(r"</?span[^>]*>", "", raw_html)

    # Remove leading/trailing whitespace and normalize internal whitespace
    cleaned = re.sub(r"\s+", " ", cleaned.strip())

    # Normalize <br> tags (handle various formats like <br>, <br/>, <br />)
    cleaned = re.sub(r"<br\s*/?>", "<br>", cleaned)

    # logging.debug(f"Cleaned HTML address: {cleaned}")
    return cleaned


def parse_german_address_components(html_address: str) -> Dict[str, str]:
    """
    Parse German address components from HTML string with <br> tags.

    Expected format:
    "Street Address<br>Postal Code City<br>Country<br>State"

    Args:
        html_address (str): HTML address string with <br> tags

    Returns:
        Dict[str, str]: Parsed address components
    """
    logging.debug(f"Parsing German address: {html_address}")

    if not html_address:
        logging.warning("Empty address provided for parsing")
        return {
            "street": "",
            "postal_code": "",
            "city": "",
            "country": "",
            "state": "",
            "full_address": "",
        }

    # Clean the HTML first
    cleaned_html = clean_html_address(html_address)

    # Split by <br> tags and clean each part
    parts = [part.strip() for part in cleaned_html.split("<br>") if part.strip()]

    result = {
        "street": "",
        "postal_code": "",
        "city": "",
        "country": "",
        "state": "",
        "full_address": ", ".join(parts),
    }

    # Parse street address (first part)
    if len(parts) >= 1:
        result["street"] = parts[0]
        logging.debug(f"Extracted street: {result['street']}")

    # Parse postal code and city (second part)
    if len(parts) >= 2:
        postal_city_match = re.match(r"^(\d{4,5})\s+(.+)$", parts[1])
        if postal_city_match:
            result["postal_code"] = postal_city_match.group(1)
            result["city"] = postal_city_match.group(2)
            logging.debug(
                f"Extracted postal code: {result['postal_code']}, city: {result['city']}"
            )
        else:
            result["city"] = parts[1]
            logging.debug(f"Extracted city (no postal code detected): {result['city']}")

    # Parse country (third part)
    if len(parts) >= 3:
        result["country"] = parts[2]
        logging.debug(f"Extracted country: {result['country']}")

    # Parse state (fourth part)
    if len(parts) >= 4:
        result["state"] = parts[3]
        logging.debug(f"Extracted state: {result['state']}")

    return result


def enrich_dealer_with_address_data(dealer: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich dealer data with parsed address components.

    Args:
        dealer (Dict[str, Any]): Raw dealer data dictionary

    Returns:
        Dict[str, Any]: Enhanced dealer data with parsed address components
    """
    logging.debug(f"Enriching dealer data for: {dealer.get('company_name', 'Unknown')}")

    # Check for raw HTML address field
    raw_html_address = dealer.get("address_raw_html", "")

    if not raw_html_address:
        logging.warning(
            f"No address_raw_html field found for dealer: {dealer.get('company_name', 'Unknown')}"
        )
        return dealer

    # Parse address components
    address_components = parse_german_address_components(raw_html_address)

    # Add parsed components to dealer data
    dealer["address_raw"] = raw_html_address
    dealer["address_cleaned"] = clean_html_address(raw_html_address)
    dealer.update(address_components)

    logging.debug(
        f"Successfully enriched address data for: {dealer.get('company_name', 'Unknown')}"
    )
    return dealer


def clean_value_for_json(value, default=''):
    """
    Clean a single value for JSON serialization by handling NaN and None values.
    
    Args:
        value: The value to clean
        default: Default value to use if value is NaN or None
        
    Returns:
        Cleaned value safe for JSON serialization
    """
    if pd.isna(value) or value is None:
        return default
    return value


async def build_schema_generic(
    schema_type: str,
    schema_file_name: str,
    cleaned_html: str,
    schema_generators: List[Dict[str, Any]],
    force: bool = False
) -> List[Dict[str, Any]]:
    """
    Generic schema builder that eliminates redundant logic between build_schema_dealer and build_schema_machines.
    
    Args:
        schema_type (str): Type of schema being built (for logging)
        schema_file_name (str): Name of the schema file (e.g., "dealer_schema.json")
        cleaned_html (str): HTML content for schema generation
        schema_generators (List[Dict[str, Any]]): List of schema generator configurations
        force (bool): Force regeneration of schema
        
    Returns:
        List[Dict[str, Any]]: List of schema dictionaries
    """
    schema_file_path = os.path.join("schemas", schema_file_name)

    # Try to load existing schemas first (unless force=True)
    if os.path.exists(schema_file_path) and not force:
        try:
            existing_schemas = load_schemas_from_json(schema_file_path)
            logging.info(f"Successfully loaded {len(existing_schemas)} existing {schema_type} schemas")
            return existing_schemas
        except Exception as e:
            logging.warning(f"Failed to load existing {schema_type} schemas: {str(e)}. Will regenerate...")
            force = True  # Force regeneration if loading fails

    # Generate new schemas if no existing schemas or force=True
    if not cleaned_html and force:
        logging.warning(f"No HTML provided for {schema_type} schema generation, but force=True. Cannot generate new schemas.")
        if os.path.exists(schema_file_path):
            logging.info(f"Falling back to existing {schema_type} schema file...")
            return load_schemas_from_json(schema_file_path)
        else:
            raise ValueError(f"Cannot generate {schema_type} schemas without HTML content and no existing schema file found")

    if not cleaned_html:
        logging.warning(f"No HTML provided and no existing {schema_type} schema file found. Cannot proceed.")
        raise ValueError(f"HTML content required for {schema_type} schema generation when no existing schema file exists")

    logging.info(f"Building {schema_type} schemas...")
    generated_schemas = []

    try:
        # Process each schema generator
        for generator_config in schema_generators:
            logging.info(f"Generating {generator_config['name']} schema...")
            
            schema = JsonCssExtractionStrategy.generate_schema(
                html=cleaned_html,
                llm_config=LLMConfig(
                    provider="gemini/gemini-2.5-pro-preview-05-06",
                    api_token=os.getenv("GEMINI_API_KEY", ""),
                ),
                target_json_example=generator_config['target_json_example'],
                query=generator_config['query']
            )

            # Handle single schema or list of schemas from generator
            if isinstance(schema, dict):
                generated_schemas.append(schema)
            elif isinstance(schema, list):
                generated_schemas.extend(schema)

        if not generated_schemas:
            raise ValueError(f"No {schema_type} schemas were generated successfully")

        # Robustly save all generated schemas to JSON file
        os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
        with open(schema_file_path, "w", encoding="utf-8") as f:
            json.dump(generated_schemas, f, indent=4, ensure_ascii=False)

        logging.info(f"Successfully generated and saved {len(generated_schemas)} {schema_type} schemas to {schema_file_path}")
        return generated_schemas

    except Exception as e:
        logging.error(f"Error generating {schema_type} schemas: {str(e)}")
        # Try to fall back to existing schemas if generation fails
        if os.path.exists(schema_file_path) and not force:
            logging.info(f"{schema_type.title()} schema generation failed, falling back to existing schemas...")
            return load_schemas_from_json(schema_file_path)
        raise


async def build_schema_dealer(
    cleaned_html: str, force: bool = False
) -> List[Dict[str, Any]]:
    """
    Build or load extraction schemas for dealer data.
    Always returns the latest schemas - loads existing or generates new ones.
    Supports multiple schema generators and robustly saves all generated schemas.

    Args:
        cleaned_html (str): HTML content for schema generation (only used if force=True or no schema exists)
        force (bool): Force regeneration of dealer schema

    Returns:
        List[Dict[str, Any]]: List of schema dictionaries
    """
    schema_generators = [
        {
            "name": "Maschinensucher Dealer Card",
            "target_json_example": """{
                "company name": "...",
                "address": "...",
                "distance": "...",
                "link": "...",
                "attributes": "...",
            }""",
            "query": """The given html is the crawled html from Maschinensucher website search result. Please find the schema for dealer information in the given html. Name the schema as "Maschinensucher Dealer Card". I am interested in the dealer company name, the address(save this as html because i need to preserve the <br> format), the distance from the detected location, and the maschinensucher page link for the company.
            """,
        },
        {
            "name": "Category Text",
            "target_json_example": """{
                "text": "..."
            }""",
            "query": """The given html is the crawled html from Maschinensucher website search result. Please find the schema for extracting the category/page title text. Name the schema as "Category Text". I am interested in extracting the main category title/heading text from the page.
            """,
        }
    ]

    return await build_schema_generic(
        schema_type="dealer",
        schema_file_name="dealer_schema.json",
        cleaned_html=cleaned_html,
        schema_generators=schema_generators,
        force=force
    )


async def build_schema_machines(
    cleaned_html: str, force: bool = False
) -> List[Dict[str, Any]]:
    """
    Build or load extraction schemas for dealer's machines data.
    Always returns the latest schemas - loads existing or generates new ones.
    Supports multiple schema generators and robustly saves all generated schemas.

    Args:
        cleaned_html (str): HTML content for schema generation (only used if force=True or no schema exists)
        force (bool): Force regeneration of machine schema

    Returns:
        List[Dict[str, Any]]: List of machine schema dictionaries
    """
    schema_generators = [
        {
            "name": "Machine Category Filter",
            "target_json_example": """[
                {
                    "category_name": "...maschinen",
                    "count": 5,
                    "sub_categories": [
                        {
                            "category_name": "...maschinen",
                            "count": 5
                        }
                    ]
                }
            ]""",
            "query": """The provided HTML snippet is from a maschinensucher website and displays category filters. Please generate a schema named 'MachineCategoryFilter' to extract the category and subcategory information. For each main category, I need its name, the count of items (the number in parentheses). Each main category might have a list of subcategories. For each subcategory, I also need its name, and count of items.
            """,
        }
    ]

    return await build_schema_generic(
        schema_type="machine",
        schema_file_name="machines_schema.json",
        cleaned_html=cleaned_html,
        schema_generators=schema_generators,
        force=force
    )


def clean_machine_data(machine_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean machine data by removing unnecessary fields.
    
    Args:
        machine_data (Dict[str, Any]): Raw machine data
        
    Returns:
        Dict[str, Any]: Cleaned machine data with only category info
    """
    if not machine_data:
        return {}
        
    cleaned = {}
    
    # Keep only the category information
    if "category_name" in machine_data and not pd.isna(machine_data["category_name"]):
        cleaned["category_name"] = machine_data["category_name"]
    
    if "count" in machine_data and not pd.isna(machine_data["count"]):
        cleaned["count"] = machine_data["count"]
        
    if "sub_categories" in machine_data:
        sub_categories = machine_data["sub_categories"]
        # Handle different types of sub_categories values
        if sub_categories is None:
            # Skip if sub_categories is None
            pass
        elif isinstance(sub_categories, (list, tuple)):
            # Handle lists/tuples first to avoid pd.isna() issues
            if len(sub_categories) == 0:
                # Empty list - skip adding sub_categories to cleaned data
                pass
            else:
                # Clean sub-categories as well
                cleaned_sub_categories = []
                for sub_cat in sub_categories:
                    if isinstance(sub_cat, dict):
                        cleaned_sub_cat = {}
                        if "category_name" in sub_cat:
                            cleaned_sub_cat["category_name"] = sub_cat["category_name"]
                        if "count" in sub_cat:
                            cleaned_sub_cat["count"] = sub_cat["count"]
                        # Use len() instead of truthiness check to avoid numpy array issues
                        if len(cleaned_sub_cat) > 0:  # Only add if not empty
                            cleaned_sub_categories.append(cleaned_sub_cat)
                
                # Only add if we have valid sub-categories
                if len(cleaned_sub_categories) > 0:
                    cleaned["sub_categories"] = cleaned_sub_categories
        elif isinstance(sub_categories, str):
            # Skip if sub_categories is a string (not a list/dict structure)
            pass
        elif pd.isna(sub_categories):
            # Handle NaN values (this should be safe now that we've handled lists first)
            pass
    
    return cleaned


def process_machine_data_for_dealer(machines_df: pd.DataFrame, dealer_id: str, max_subcategories: int = None) -> Dict[str, Any]:
    """
    Process machine data for a specific dealer and generate category analytics with dynamic subcategory handling.
    
    Args:
        machines_df (pd.DataFrame): DataFrame containing machine data
        dealer_id (str): Dealer ID to process
        max_subcategories (int): Maximum number of subcategories to process (if None, process all)
    
    Returns:
        Dict[str, Any]: Dictionary containing main category and subcategory analytics
    """
    # Filter machine data for this dealer
    dealer_machines = machines_df[machines_df['dealer_id'] == dealer_id]
    
    if dealer_machines.empty:
        return {}
    
    analytics = {}
    
    # Process each machine record for this dealer
    for _, machine in dealer_machines.iterrows():
        try:
            # Clean the machine data first
            machine_dict = machine.to_dict()
            cleaned_machine = clean_machine_data(machine_dict)
            
            if not cleaned_machine:
                continue
                
            # Extract main category information
            main_category = cleaned_machine.get('category_name', '')
            main_count = cleaned_machine.get('count', 0)
            
            if main_category and not pd.isna(main_category):
                analytics['main_category'] = str(main_category)
                analytics['main_category_count'] = str(main_count) if not pd.isna(main_count) else '0'
              # Extract subcategory information with dynamic limit
            sub_categories = cleaned_machine.get('sub_categories', [])
            if sub_categories and isinstance(sub_categories, (list, tuple)):
                # Use provided max or process all subcategories
                limit = max_subcategories if max_subcategories else len(sub_categories)
                
                for i, sub_cat in enumerate(sub_categories[:limit], 1):
                    if isinstance(sub_cat, dict):
                        sub_name = sub_cat.get('category_name', '')
                        sub_count = sub_cat.get('count', 0)
                        
                        if sub_name and not pd.isna(sub_name):
                            analytics[f'sub_category_{i}'] = str(sub_name)
                            analytics[f'sub_category_{i}_count'] = str(sub_count) if not pd.isna(sub_count) else '0'
        
        except Exception as e:
            logging.warning(f"Error processing machine data for dealer {dealer_id}: {str(e)}")
            continue
    
    return analytics


def generate_category_analytics_columns(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate category analytics columns from machine data with dynamic subcategory limits.
    
    Args:
        merged_df (pd.DataFrame): Merged DataFrame containing dealer and machine data
    
    Returns:
        pd.DataFrame: DataFrame with added category analytics columns
    """
    logging.info("Generating category analytics columns from machine data")
    
    # Create a copy to avoid modifying the original
    enhanced_df = merged_df.copy()
    
    # Create a temporary DataFrame with just machine data columns
    machine_columns = ['dealer_id', 'category_name', 'count', 'sub_categories']
    available_machine_columns = [col for col in machine_columns if col in enhanced_df.columns]
    
    # Dynamically determine the maximum number of subcategories needed
    if 'sub_categories' in enhanced_df.columns:
        machines_df = enhanced_df[available_machine_columns].dropna(subset=['dealer_id'])
        max_subcategories = determine_max_subcategories(machines_df)
        logging.info(f"Detected maximum subcategories needed: {max_subcategories}")
    else:
        max_subcategories = 0
        logging.warning("No sub_categories column found, setting max subcategories to 0")
    
    # Initialize category analytics columns dynamically
    category_columns = ['main_category', 'main_category_count']
    
    # Add subcategory columns based on detected maximum
    for i in range(1, max_subcategories + 1):
        category_columns.extend([f'sub_category_{i}', f'sub_category_{i}_count'])
    
    # Initialize all category columns with empty strings
    for col in category_columns:
        enhanced_df[col] = ''
      # Get unique dealer IDs that have machine data
    dealers_with_machines = enhanced_df[enhanced_df['dealer_id'].notna() & (enhanced_df['dealer_id'] != '')]
    
    if dealers_with_machines.empty:
        logging.warning("No dealers with machine data found for category analytics")
        return enhanced_df

    if len(available_machine_columns) <= 1:  # Only dealer_id
        logging.warning("No machine data columns found for category analytics")
        return enhanced_df

    # Process each dealer's machine data
    processed_dealers = 0
    for dealer_id in dealers_with_machines['dealer_id'].unique():
        if pd.isna(dealer_id) or dealer_id == '':
            continue
            
        # Generate analytics for this dealer with dynamic subcategory limit
        analytics = process_machine_data_for_dealer(machines_df, str(dealer_id), max_subcategories)
        
        if analytics:
            # Update the DataFrame with analytics for this dealer
            dealer_mask = enhanced_df['dealer_id'] == dealer_id
            for col, value in analytics.items():
                if col in enhanced_df.columns:
                    enhanced_df.loc[dealer_mask, col] = value
            processed_dealers += 1
    
    logging.info(f"Generated category analytics for {processed_dealers} dealers")
    return enhanced_df


def prepare_csv(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare and clean merged dealer and machine data for CSV export with dynamic subcategory handling.
    
    Args:
        merged_df (pd.DataFrame): Merged DataFrame containing dealer and machine data

    Returns:
        pd.DataFrame: Cleaned DataFrame ready for CSV export
    """
    if merged_df.empty:
        logging.warning("No merged data to prepare for CSV")
        return pd.DataFrame()

    logging.info(f"Preparing {len(merged_df)} records for CSV export")
    
    # Create a copy to avoid modifying the original
    prepared_df = merged_df.copy()
    
    # Handle vertrauensiegel field - check if attributes contains "Vertrauenssiegel"
    if 'attributes' in prepared_df.columns:
        prepared_df['vertrauensiegel'] = prepared_df['attributes'].fillna('').str.lower().str.contains('vertrauenssiegel')
    else:
        prepared_df['vertrauensiegel'] = False
    # Log first few entries with vertrauensiegel
    if 'vertrauensiegel' in prepared_df.columns:
        vertrauensiegel_dealers = prepared_df[prepared_df['vertrauensiegel']].head(5)
        for _, dealer in vertrauensiegel_dealers.iterrows():
            logging.debug(f"Dealer with Vertrauenssiegel: {dealer.get('company_name', 'Unknown')}")
    
    # Define the essential fields to keep in CSV
    essential_fields = [
        "vertrauensiegel",
        "company_name", 
        "street",
        "postal_code",
        "city",
        "state", 
        "country",
        "distance",
        "link",
        "category",
        "category_id",
        "page_number",
        "source_url",
        "dealer_id",
        "contact_person",
        "phone_number",
        "fax_number",
        "main_category",
        "main_category_count"
    ]
    
    # Dynamically add subcategory fields based on what's available in the DataFrame
    subcategory_columns = [col for col in prepared_df.columns if col.startswith('sub_category_')]
    essential_fields.extend(subcategory_columns)
    
    # Keep only essential fields that exist in the DataFrame
    available_fields = [field for field in essential_fields if field in prepared_df.columns]
    final_df = prepared_df[available_fields].copy()
    
    # Fill missing values with empty strings for CSV compatibility
    final_df = final_df.fillna('')
    
    logging.info(f"Prepared {len(final_df)} dealer records for CSV export")
    logging.info(f"CSV fields: {', '.join(final_df.columns.tolist())}")
    
    # Log the number of dynamic subcategory columns detected
    num_subcategory_cols = len([col for col in final_df.columns if col.startswith('sub_category_')])
    logging.info(f"Detected {num_subcategory_cols} dynamic subcategory columns")

    return final_df


def save_to_csv(prepared_df: pd.DataFrame, filename: str) -> bool:
    """
    Save prepared dealer DataFrame to a CSV file with BOM for German language compatibility.

    Args:
        prepared_df (pd.DataFrame): DataFrame with cleaned dealer data
        filename (str): Output CSV filename

    Returns:
        bool: True if file was saved successfully, False otherwise
    """
    if prepared_df.empty:
        logging.warning("No prepared data to save to CSV")
        return False

    try:
        # Save DataFrame to CSV with BOM for German language compatibility
        prepared_df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logging.info(f"Dealer data successfully saved to CSV with BOM: {filename}")
        logging.info(f"Saved {len(prepared_df)} records with fields: {', '.join(prepared_df.columns.tolist())}")
        return True

    except Exception as e:
        logging.error(f"Error saving data to CSV file '{filename}': {str(e)}")
        return False





def prepare_json_output(merged_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Prepare merged DataFrame for JSON output with cleaned machine data structure.
    
    Args:
        merged_df (pd.DataFrame): Merged DataFrame containing dealer and machine data
    
    Returns:
        List[Dict[str, Any]]: List of dictionaries ready for JSON export
    """
    logging.info("Preparing data for JSON output with cleaned machine data")
    
    json_output = []
    
    # Group by dealer_id to process each dealer separately
    for dealer_id, dealer_group in merged_df.groupby('dealer_id'):
        if pd.isna(dealer_id) or dealer_id == '':
            continue
            
        # Get the first row for dealer info (all rows for same dealer should have same dealer info)
        dealer_row = dealer_group.iloc[0]
          # Create dealer entry with proper NaN handling
        dealer_entry = {}
        for field, default in [
            ('company_name', ''),
            ('street', ''),
            ('postal_code', ''),
            ('city', ''),
            ('state', ''),
            ('country', ''),
            ('distance', ''),
            ('link', ''),
            ('category', ''),
            ('category_id', ''),
            ('attributes', ''),
            ('vertrauensiegel', False)
        ]:
            value = dealer_row.get(field, default)
            # Handle pandas NaN values
            if pd.isna(value):
                dealer_entry[field] = default
            else:
                dealer_entry[field] = value
        
        dealer_entry['dealer_id'] = str(dealer_id)
        
        # Process machine data if available
        machine_data_rows = dealer_group[dealer_group['category_name'].notna()]
        
        if not machine_data_rows.empty:            # Process each machine data entry for this dealer
            for _, machine_row in machine_data_rows.iterrows():
                # Clean machine row data to handle NaN values
                machine_dict = {}
                for key, value in machine_row.to_dict().items():
                    machine_dict[key] = clean_value_for_json(value, '')
                
                cleaned_machine = clean_machine_data(machine_dict)
                
                if cleaned_machine:
                    # Create a combined entry with dealer info + cleaned machine data
                    combined_entry = dealer_entry.copy()
                    combined_entry.update({
                        'category_name': clean_value_for_json(cleaned_machine.get('category_name'), ''),
                        'count': str(clean_value_for_json(cleaned_machine.get('count'), '')),
                        'sub_categories': cleaned_machine.get('sub_categories', []),
                        'source_url': clean_value_for_json(machine_row.get('source_url'), ''),
                        'category_id_filter': clean_value_for_json(machine_row.get('category_id_filter'), ''),
                        'page_number': clean_value_for_json(machine_row.get('page_number'), '')
                    })
                    json_output.append(combined_entry)
        else:
            # If no machine data, add dealer entry without machine info
            json_output.append(dealer_entry)
    
    logging.info(f"Prepared {len(json_output)} entries for JSON output")
    return json_output


def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame by replacing NaN values with appropriate defaults for JSON serialization.
    
    Args:
        df (pd.DataFrame): DataFrame to clean
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with no NaN values
    """
    logging.info("Cleaning DataFrame for JSON export - replacing NaN values")
    
    # Create a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # Replace NaN values with appropriate defaults based on column type and name
    for column in cleaned_df.columns:
        if cleaned_df[column].dtype == 'object':
            # For string/object columns, replace with empty string
            cleaned_df[column] = cleaned_df[column].fillna('')
        elif cleaned_df[column].dtype in ['int64', 'float64']:
            # For numeric columns, replace with 0 or empty string depending on context
            if 'count' in column.lower() or 'number' in column.lower():
                cleaned_df[column] = cleaned_df[column].fillna(0)
            else:
                cleaned_df[column] = cleaned_df[column].fillna('')
        elif cleaned_df[column].dtype == 'bool':
            # For boolean columns, replace with False
            cleaned_df[column] = cleaned_df[column].fillna(False)
        else:
            # For any other type, replace with empty string
            cleaned_df[column] = cleaned_df[column].fillna('')
    
    # Special handling for specific fields that should have meaningful defaults
    if 'vertrauensiegel' in cleaned_df.columns:
        cleaned_df['vertrauensiegel'] = cleaned_df['vertrauensiegel'].fillna(False)
    
    if 'attributes' in cleaned_df.columns:
        cleaned_df['attributes'] = cleaned_df['attributes'].fillna('')
    
    logging.info(f"Successfully cleaned {len(cleaned_df)} records for JSON export")
    return cleaned_df


def determine_max_subcategories(machines_df: pd.DataFrame) -> int:
    """
    Analyze the machine data to determine the maximum number of subcategories
    needed across all dealers.
    
    Args:
        machines_df: DataFrame containing machine data with sub_categories column
        
    Returns:
        int: Maximum number of subcategories found
    """
    max_subcategories = 0
    
    if 'sub_categories' not in machines_df.columns:
        return max_subcategories
    
    for _, row in machines_df.iterrows():
        sub_categories = row.get('sub_categories', [])
        if isinstance(sub_categories, (list, tuple)):
            max_subcategories = max(max_subcategories, len(sub_categories))
        elif isinstance(sub_categories, str):
            try:
                # Handle string representation of lists
                parsed_sub_cats = json.loads(sub_categories)
                if isinstance(parsed_sub_cats, list):
                    max_subcategories = max(max_subcategories, len(parsed_sub_cats))
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    
    # Add some buffer for safety (e.g., 20% more or minimum 5)
    buffer = max(int(max_subcategories * 0.2), 5)
    return max_subcategories + buffer

async def main():
    """
    Main function for scraping dealers and their machine data using pandas DataFrames.
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Scrape dealers and their machines from maschinensucher.de"
    )
    parser.add_argument(
        "--category",
        type=str,
        default="2",
        help="Category code to scrape dealers from (default: all categories)",
    )
    parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages to scrape (default: 1)"
    )
    parser.add_argument(
        "--force-schema",
        action="store_true",
        default=False,
        help="Force regeneration of dealer schema",
    )
    parser.add_argument(
        "--contact",
        action="store_true",
        default=False,
        help="Extract detailed contact information for dealers (phone, fax, contact person)",
    )

    args = parser.parse_args()

    try:
        # Run the actual dealer scraping
        logging.info(f"Starting dealer scraping for {args.pages} page(s)...")
        dealers_list = await grab_dealer_list(num_pages=args.pages, category_code=args.category)

        if not dealers_list:
            logging.warning("No dealers found. Exiting.")
            print("\n=== SCRAPING RESULTS ===")
            print("No dealers found.")
            return

        # Convert dealers list to DataFrame
        dealers_df = pd.DataFrame(dealers_list)
        logging.info(f"Created dealers DataFrame with {len(dealers_df)} records")

        # Create dealer_id column by extracting from link column
        dealers_df['dealer_id'] = dealers_df['link'].apply(extract_dealer_id_from_link)
        logging.info("Added dealer_id column to dealers DataFrame")

        # Filter out dealers without valid dealer_id
        valid_dealers_df = dealers_df[dealers_df['dealer_id'] != ''].copy()
        logging.info(f"Found {len(valid_dealers_df)} dealers with valid dealer IDs")

        if valid_dealers_df.empty:
            logging.warning("No dealers with valid IDs found. Exiting.")
            print("\n=== SCRAPING RESULTS ===")
            print("No dealers with valid IDs found.")
            return        # Get machine data using parallel crawling (regardless of --parallel flag)
        logging.info(f"Grabbing machine data for {len(valid_dealers_df)} dealers...")
        dealer_ids_list = valid_dealers_df['dealer_id'].tolist()
        # Use the first dealer's category_id for machine crawling, or use args.category
        category_code = args.category if args.category else valid_dealers_df['category_id'].iloc[0] if 'category_id' in valid_dealers_df.columns else ""
        
        machines_data = await grab_dealer_machines_parallel(
            dealer_ids=dealer_ids_list,
            category_code=category_code,
            num_pages=1
        )
        
        logging.info(f"Retrieved machine data: {len(machines_data)} records")
        
        # Get detailed dealer contact information (only if --contact flag is provided)
        contact_info_data = pd.DataFrame()  # Initialize empty DataFrame
        if args.contact:
            logging.info(f"Grabbing detailed contact information for {len(valid_dealers_df)} dealers...")
            dealer_links_list = valid_dealers_df['link'].tolist()
            contact_info_data = await grab_dealer_info_sequential(dealer_links=dealer_links_list)
            logging.info(f"Retrieved contact information: {len(contact_info_data)} records")
        else:
            logging.info("Skipping contact information extraction (--contact flag not provided)")

        # Merge dealers data with machines data on dealer_id
        if not machines_data.empty:
            merged_df = pd.merge(dealers_df, machines_data, on='dealer_id', how='left')
            logging.info(f"Merged dealers and machines data: {len(merged_df)} records")
        else:
            merged_df = dealers_df.copy()
            logging.warning("No machine data found, using dealers data only")
        
        # Merge with contact information data on dealer_id
        if not contact_info_data.empty:
            # Rename 'id' column to 'dealer_id' in contact_info_data for merging
            contact_info_data_renamed = contact_info_data.rename(columns={'id': 'dealer_id'})
            merged_df = pd.merge(merged_df, contact_info_data_renamed, on='dealer_id', how='left')
            logging.info(f"Merged with contact information: {len(merged_df)} records")
        else:
            # Add empty contact columns if no contact data was retrieved
            merged_df['contact_person'] = ''
            merged_df['phone_number'] = ''
            merged_df['fax_number'] = ''
            logging.warning("No contact information found, added empty contact columns")

        # Save results to files
        if not merged_df.empty:
            # Create output directory if it doesn't exist
            output_dir = os.path.join("output")
            os.makedirs(output_dir, exist_ok=True)            # Generate filename with category and timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            category_suffix = f"_{args.category}" if args.category else ""
              # Save JSON with original structure (without flattened category columns)
            output_json_file = os.path.join(
                output_dir, f"dealer_results{category_suffix}_{timestamp}.json"
            )
            # Clean DataFrame to remove NaN values before JSON conversion
            cleaned_for_json = clean_dataframe_for_json(merged_df)
            json_ready_data = cleaned_for_json.to_dict('records')
            with open(output_json_file, 'w', encoding='utf-8') as f:
                json.dump(json_ready_data, f, indent=4, ensure_ascii=False)
            logging.info(f"Results saved to JSON: {output_json_file}")            # Generate category analytics columns ONLY for CSV
            csv_df = generate_category_analytics_columns(merged_df)
            logging.info("Added category analytics columns for CSV output")
            
            # Prepare and save CSV with flattened category columns
            csv_filename = os.path.join(output_dir, f"dealer_results{category_suffix}_{timestamp}.csv")
            prepared_df = prepare_csv(csv_df)
            success = save_to_csv(prepared_df, csv_filename)

            if not success:
                logging.error(f"Failed to save CSV file: {csv_filename}")
            else:
                logging.info(f"Successfully saved CSV file: {csv_filename}")

        print("\n=== SCRAPING RESULTS ===")
        print(f"Total dealers processed: {len(merged_df)}")
        
        # Summary of machine data
        if not machines_data.empty:
            dealers_with_machines = len(merged_df[merged_df['dealer_id'].isin(machines_data['dealer_id'])])
            print(f"Dealers with machine data: {dealers_with_machines}/{len(merged_df)}")
            print(f"Total machine records found: {len(machines_data)}")
        else:
            print("No machine data found")
        
        # Summary of contact information data
        if args.contact:
            if not contact_info_data.empty:
                dealers_with_contact_info = len(merged_df[(merged_df['contact_person'] != '') | 
                                                         (merged_df['phone_number'] != '') | 
                                                         (merged_df['fax_number'] != '')])
                print(f"Dealers with contact information: {dealers_with_contact_info}/{len(merged_df)}")
                print(f"Total contact records found: {len(contact_info_data)}")
            else:
                print("No contact information found")
        else:
            print("Contact information extraction was skipped (use --contact to enable)")
        
        if not merged_df.empty:
            print("Results saved to:")
            print(f"  - JSON: {os.path.abspath(output_json_file)}")
            print(f"  - CSV:  {os.path.abspath(csv_filename)}")
        else:
            print("No data found to save.")

        logging.info("Dealer and machine scraping completed successfully!")

    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )
    asyncio.run(main())
    #asyncio.run(grab_dealer_info_sequential(dealer_links=[ "/Haendler/47558/wmw-ag-leipzig", "/Haendler/49713/dynamic-power-laser-gmbh-leipzig", "/Haendler/66037/iob-industrieofenanlagen-vertrieb-gmbh-hartha", "/Haendler/46836/mhl-maschinenhandel-andreas-ludewig-bad-sulza", "/Haendler/86079/maveg-maschinen-vertriebs-gesellschaft-mbh-chemnitz"]))
