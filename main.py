# main.py

import sys
from steam_fetcher.gui import start_gui # Import the GUI starter function
# from steam_fetcher.data_handler import DataHandler # Keep for later

def main():
    """Main function to start the application."""
    print("Starting Steam Game Fetcher GUI...")
    # Initialize GUI and start the main loop
    start_gui()

if __name__ == "__main__":
    main()
