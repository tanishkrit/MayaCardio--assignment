#!/usr/bin/env python3
"""
Top-level entrypoint for pipeline CLI.
"""
import sys
import os

# Add workspace directory to python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from device.cli import main

if __name__ == "__main__":
    main()
