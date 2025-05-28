import logging
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

