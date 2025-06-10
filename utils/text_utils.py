import re
from typing import Optional


def clean_text(text: str) -> str:
    """Clean and normalize text content"""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove excessive newlines
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Remove control characters except common ones
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    
    return text


def extract_title_from_content(content: str, max_length: int = 100) -> Optional[str]:
    """Extract a meaningful title from content"""
    if not content or len(content.strip()) == 0:
        return None
    
    lines = content.split('\n')
    
    # Look for the first non-empty line that could be a title
    for line in lines[:5]:  # Check first 5 lines
        line = line.strip()
        if len(line) > 5 and len(line) <= max_length:
            # Skip lines that look like metadata
            if not any(skip_pattern in line.lower() for skip_pattern in [
                'created:', 'modified:', 'author:', 'date:', 'source:', 'url:'
            ]):
                return line
    
    # If no suitable line found, use first 100 characters
    first_sentence = content.strip()[:max_length]
    if '.' in first_sentence:
        first_sentence = first_sentence.split('.')[0] + '.'
    elif '\n' in first_sentence:
        first_sentence = first_sentence.split('\n')[0]
    
    return first_sentence.strip() if first_sentence.strip() else None


def extract_keywords(text: str, max_keywords: int = 10) -> list[str]:
    """Extract keywords from text content"""
    if not text:
        return []
    
    # Simple keyword extraction
    # Remove common stop words
    stop_words = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
        'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
        'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this',
        'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'
    }
    
    # Extract words
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    
    # Filter stop words and count frequency
    word_freq = {}
    for word in words:
        if word not in stop_words and len(word) > 2:
            word_freq[word] = word_freq.get(word, 0) + 1
    
    # Sort by frequency and return top keywords
    sorted_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, freq in sorted_keywords[:max_keywords]]


def truncate_text(text: str, max_length: int = 1000) -> str:
    """Truncate text to specified length while preserving word boundaries"""
    if len(text) <= max_length:
        return text
    
    truncated = text[:max_length]
    
    # Find last space to avoid cutting words
    last_space = truncated.rfind(' ')
    if last_space > max_length * 0.8:  # Only if we don't lose too much
        truncated = truncated[:last_space]
    
    return truncated + "..."


def validate_text_content(text: str) -> tuple[bool, Optional[str]]:
    """Validate text content"""
    if not text or not text.strip():
        return False, "Text content is empty"
    
    if len(text.strip()) < 10:
        return False, "Text content is too short (minimum 10 characters)"
    
    # Check for reasonable character distribution
    printable_chars = sum(1 for c in text if c.isprintable() or c.isspace())
    if printable_chars / len(text) < 0.8:
        return False, "Text contains too many non-printable characters"
    
    return True, None