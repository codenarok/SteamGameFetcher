# Steam Game Fetcher

## Project Goal

Develop a Python desktop application named "Steam Game Fetcher" using Object-Oriented Programming principles to retrieve Steam game compatibility data.

## Core Functionality

The application presents a Graphical User Interface (GUI) upon launch with options to fetch Steam game data.

### Option 1: Targeted Game Data Retrieval (Planned/Future)

*   **UI Element:** "Get Steam Data for Listed Games" (Button/Menu Item)
*   **Functionality:** Allows the user to select an input CSV file containing game identifiers. The application will then fetch corresponding data specifically for those games from Steam or related sources. (Note: This functionality might require further implementation).

### Option 2: Comprehensive Steam Data Scraping (Implemented)

*   **UI Element:** "Run Full Steam Data Scrape" (Button/Menu Item)
*   **Functionality:** Executes a web scraping process targeting `https://checkmydeck.ofdgn.com/all-games` to gather Steam Deck compatibility information for a large number of games.
*   **Important:** Due to anti-scraping measures (like Cloudflare) on the target website, this option requires manually launching Google Chrome with remote debugging enabled *before* running the scrape.

### Option 3: Fetch Titles from DB & Store to Mongo (Placeholder/Example)

*   **UI Element:** "Fetch Titles from DB & Store to Mongo" (Button/Menu Item)
*   **Functionality:** (This is likely a placeholder or specific internal task) Fetches data from one database, potentially scrapes related info, and stores it in MongoDB. Requires configuration (`config.ini`) for database credentials.

### Option 4: Insert CSV Data into Azure SQL Database (Implemented)

*   **UI Element:** "Insert CSV to Azure SQL DB" (Button/Menu Item)
*   **Functionality:** Allows the user to select a local CSV file. The application then connects to an Azure SQL Database (`certdatavalidation.database.windows.net`, Database: `DataValidation`) using Azure Managed Identity authentication. It validates if the CSV columns match the `[dbo].[SteamOSHandheldInfo]` table (case-insensitive) and inserts the data in batches if validation passes.
*   **Prerequisites:**
    *   The user running the application must have permissions to connect to the specified Azure SQL database via Managed Identity.
    *   The user must have INSERT permissions on the `[dbo].[SteamOSHandheldInfo]` table.
    *   The CSV file structure must match the table structure (column names and order, case-insensitive).
*   **Output:** Status messages and a progress bar are shown in the GUI. A final success or error message is displayed.

## Technical Requirements

*   **Language:** Python 3
*   **GUI:** Tkinter (Standard Python library)
*   **Web Scraping:** Playwright with `playwright-stealth` (to mimic a real browser)
*   **Data Handling:** Pandas
*   **Database:** `pyodbc` for Azure SQL connection
*   **Output:** CSV (`scraped_data.csv` for Option 2), Status updates in GUI

## Setup Instructions

1.  **Prerequisites:**
    *   Python 3 installed (e.g., from python.org).
    *   `pip` (Python package installer, usually comes with Python).
    *   Google Chrome browser installed.
2.  **Clone Repository (Optional):** If you haven't already, clone the project repository to your local machine.
3.  **Navigate to Project Directory:** Open your terminal or command prompt and change directory to the project folder:
    ```bash
    cd "c:\Users\t-matasert\source\repos\Steam Game Fetcher"
    ```
4.  **Install Dependencies:** Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

### Running the "Full Steam Data Scrape" (Requires Manual Steps)

This method is necessary to bypass potential Cloudflare challenges.

1.  **Launch Chrome Manually with Debugging:**
    *   Open **PowerShell** (or Command Prompt).
    *   Run the following command (adjust the path if your Chrome is installed elsewhere):
        ```powershell
        & "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
        ```
    *   This will open a new Chrome window controlled by the command line.

2.  **Prepare the Browser:**
    *   In the **newly opened Chrome window**, navigate to: `https://checkmydeck.ofdgn.com/all-games?sort=deck-compat-last-change-desc`
    *   If you see a Cloudflare "Checking your browser" or "I am human" challenge, **complete it manually**.
    *   **Leave this Chrome window open.**

3.  **Run the Python Application:**
    *   In your terminal (in the project directory), run:
        ```bash
        python main.py
        ```
    *   The application's GUI window should appear.

4.  **Start the Scrape:**
    *   Click the "Run Full Steam Data Scrape" button in the application GUI.
    *   The script will connect to the Chrome instance you opened manually and begin scraping. You will see progress messages in the terminal where you ran `python main.py`.

5.  **Completion:** The script will scroll through the website, collecting data in batches and saving it to `scraped_data.csv`. It will stop when it detects the end of the data or if an error occurs. The manual Chrome window can be closed after the script prints "Disconnected from browser."

### Running "Get Steam Data for Listed Games" (Planned)

*   Run the application: `python main.py`
*   Click the "Get Steam Data for Listed Games" button.
*   Use the file browser to select your input CSV file.
*   The application will process the file and fetch the data. (Requires implementation).

### Running "Fetch Titles from DB & Store to Mongo" (Placeholder)

*   Run the application: `python main.py`
*   Ensure your `config.ini` is correctly set up with database credentials if required by this specific function.
*   Click the "Fetch Titles from DB & Store to Mongo" button.
*   Monitor the console and GUI for status updates.

### Running "Insert CSV to Azure SQL DB"

*   Run the application: `python main.py`
*   Ensure your Azure environment is configured correctly for Managed Identity authentication to the target SQL database.
*   Click the "Insert CSV to Azure SQL DB" button.
*   Use the file browser to select the CSV file containing data for the `[dbo].[SteamOSHandheldInfo]` table.
*   The application will validate the columns and attempt to insert the data. Monitor the GUI for progress and status messages.

## Output File (`scraped_data.csv`)

The scraped data is saved in the `scraped_data.csv` file within the project directory. The columns are:

1.  **Row Number:** The index of the row from the source website (used for tracking/resuming).
2.  **Last Change:** Date the compatibility status was last updated.
3.  **Title:** The name of the game.
4.  **Developer:** The game's developer(s).
5.  **Reviews:** User review score percentage.
6.  **Price:** Current price listed.
7.  **Discount:** Discount percentage, if any.
8.  **ProtonDB:** ProtonDB compatibility rating (e.g., Gold, Platinum).
9.  **SteamOSResultStatus:** Derived Steam Deck compatibility status (Verified, Playable, Unsupported, Unknown).

## Troubleshooting

*   **Cloudflare Issues:** If the scrape fails immediately or gets stuck, ensure you followed the manual Chrome launch steps correctly, including passing the Cloudflare check *before* clicking the scrape button in the app.
*   **Connection Errors:** Ensure Chrome was launched with `--remote-debugging-port=9222` and is still running when you start the scrape.
*   **CSV Errors:** If you encounter issues resuming a scrape, you might need to delete or backup the existing `scraped_data.csv` file and start fresh.
*   **Azure SQL Connection Issues:**
    *   Verify that the machine running the script has network access to `certdatavalidation.database.windows.net`.
    *   Ensure the Managed Identity (either system-assigned or user-assigned for the environment where the script runs, e.g., an Azure VM, Azure App Service, or local machine with Azure CLI logged in) has been granted appropriate permissions (e.g., `db_datareader`, `db_datawriter` or specific INSERT permissions) on the `DataValidation` database.
    *   Check if the necessary ODBC Driver (ODBC Driver 17 or 18 for SQL Server) is installed on the system. The connection string specifies Driver 18.
*   **CSV Validation Errors:** Double-check that the column names and order in your CSV file exactly match the columns in the `[dbo].[SteamOSHandheldInfo]` table, ignoring only case differences.
*   **Data Insertion Errors:** Look at the specific database error message provided in the GUI or console. This could indicate data type mismatches, constraint violations, or other SQL issues.
