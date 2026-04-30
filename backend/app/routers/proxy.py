from fastapi import APIRouter, HTTPException, Response
import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os

# Get a logger instance
logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/")
async def proxy_url(url: str):
    """
    Proxies a URL to avoid CORS issues. Fetches the content of the given URL
    and injects a <base> tag to ensure relative links work correctly.
    """
    logger.info(f"Proxying URL: {url}")
    try:
        headers = {
            "User-Agent": "YourAppName/1.0 (your.email@example.com)",
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        content = response.content
        media_type = response.headers.get('content-type', 'text/html')

        if 'text/html' in media_type:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Create and inject the <base> tag
            parsed_url = urlparse(url)
            base_href = f"{parsed_url.scheme}://{parsed_url.netloc}{os.path.dirname(parsed_url.path)}/"
            
            base_tag = soup.new_tag('base', href=base_href)
            
            head = soup.find('head')
            if head:
                head.insert(0, base_tag)
            else:
                # If no head, create one and add it
                head = soup.new_tag('head')
                soup.html.insert(0, head)
                head.append(base_tag)
            
            content = str(soup)

        return Response(content=content, media_type=media_type)

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL '{url}': {e}")
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")
