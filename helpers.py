import asyncio
import logging
import random
import re
from typing import Any


def decode_unicode_escapes(data: Any) -> Any:
    """
    Recursively decode Unicode escape sequences in strings within data structures.
    
    This fixes issues where crawl4ai or other libraries return data with 
    Unicode escape sequences like \\u00fc instead of proper UTF-8 characters.
    
    Args:
        data: Data structure that may contain Unicode escape sequences
        
    Returns:
        Data structure with Unicode escape sequences decoded to proper UTF-8
    """
    if isinstance(data, str):
        # Check if string contains Unicode escape sequences like \u00fc
        if '\\u' in data:
            try:
                # Use codecs.decode to handle Unicode escape sequences
                import codecs
                decoded = codecs.decode(data, 'unicode_escape')
                logging.debug(f"Decoded Unicode escapes: {repr(data)} -> {repr(decoded)}")
                return decoded
            except (UnicodeDecodeError, UnicodeEncodeError) as e:
                # If decoding fails, return original string
                logging.debug(f"Failed to decode Unicode escapes in: {repr(data)}, error: {e}")
                return data
        else:
            # No Unicode escapes, return as-is
            return data
    elif isinstance(data, dict):
        decoded_dict = {}
        for key, value in data.items():
            # Skip Unicode decoding for HTML and raw content fields to avoid breaking HTML parsing
            if key in ['address_raw_html', 'address_raw', 'address_cleaned']:
                decoded_dict[key] = value
            else:
                decoded_dict[key] = decode_unicode_escapes(value)
        return decoded_dict
    elif isinstance(data, list):
        return [decode_unicode_escapes(item) for item in data]
    else:
        return data
    
def is_valid_phone_number(phone_str: str) -> bool:
    """
    Check if a phone string contains a valid phone number.
    Returns False if it contains error messages or is not a proper phone number.
    
    Args:
        phone_str (str): Phone number string to validate
        
    Returns:
        bool: True if valid phone number, False otherwise
    """
    if not phone_str or not phone_str.strip():
        return False
    
    phone_str = phone_str.strip()
    
    # Check for German error messages
    error_messages = [
        "Die Telefonnummer kann aktuell nicht abgerufen werden",
        "bitte versuchen Sie es später erneut",
        "Telefonnummer nicht verfügbar",
        "nicht erreichbar",
        "temporarily unavailable",
        "not available"
    ]
    
    # If any error message is found, it's not a valid phone number
    for error_msg in error_messages:
        if error_msg.lower() in phone_str.lower():
            return False
    
    # Basic phone number pattern validation (German and international formats)
    # Allow numbers with +, spaces, hyphens, parentheses, and typical separators
    phone_pattern = r'^[\+]?[\d\s\-\(\)\/\.]{7,20}$'
    
    # Remove common separators for pattern matching
    cleaned_phone = re.sub(r'[\s\-\(\)\/\.]', '', phone_str)
    
    # Must contain at least 7 digits and not be all zeros
    if len(cleaned_phone) < 7 or cleaned_phone.replace('+', '').replace('0', '') == '':
        return False
    
    return bool(re.match(phone_pattern, phone_str))

async def exponential_backoff_delay(attempt: int, base_delay: float = 1.0, max_delay: float = 30.0) -> None:
    """
    Apply exponential backoff delay with jitter.
    
    Args:
        attempt (int): Current attempt number (0-based)
        base_delay (float): Base delay in seconds
        max_delay (float): Maximum delay in seconds
    """
    # Calculate exponential delay: base_delay * 2^attempt
    delay = min(base_delay * (2 ** attempt), max_delay)
    
    # Add jitter (±25% randomness) to prevent thundering herd
    jitter = delay * 0.25 * (2 * random.random() - 1)
    final_delay = max(0, delay + jitter)
    
    logging.debug(f"Applying exponential backoff: attempt {attempt + 1}, delay {final_delay:.2f}s")
    await asyncio.sleep(final_delay)

def extract_dealer_id_from_link(link: str) -> str:
    """
    Extract dealer ID from dealer link.
    
    Args:
        link (str): Dealer link in format /Haendler/47038/imexx-systemtechnik-ronnenberg
        
    Returns:
        str: Dealer ID (e.g., "47038")
    """
    try:
        # Split the link by "/" and get the dealer ID part
        parts = link.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "Haendler":
            return parts[1]
        else:
            logging.warning(f"Could not extract dealer ID from link: {link}")
            return ""
    except Exception as e:
        logging.error(f"Error extracting dealer ID from link '{link}': {str(e)}")
        return ""