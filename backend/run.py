#!/usr/bin/env python3
"""
Run script for SEC Filing Data Extraction API
"""

import uvicorn
from main import app

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
