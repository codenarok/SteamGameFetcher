# steam_fetcher/listed_scraper.py
import os
import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import time # Keep for potential delays if needed

class ListedGameScraper:
    """
    Scrapes game data from checkmydeck.ofdgn.com based on a list of game titles
    provided in an input CSV file. Finds an exact (case-insensitive) match for
    the title and outputs the original CSV data plus the SteamOS compatibility status.
    """
    def __init__(self, input_csv_path, output_csv_path="search_results.csv"):
        self.input_csv_path = input_csv_path
        self.output_csv_path = output_csv_path
        self.login_wait = 60000  # 60 seconds for manual login
        self.search_wait = 3000   # Reduced wait after search term input, adjust if needed
        # Assuming the game title to search for is in the first column (index 0) of the input CSV
        self.search_term_column_index = 0
        # Assuming the game title in the web grid is the second data cell (index 1)
        # Cells: [Last Change (0), Title (1), Developer (2), Reviews (3), Price (4), Discount (5), ProtonDB (6)]
        self.web_title_cell_index = 1


    def _read_input_csv(self):
        """Reads the entire input CSV into a DataFrame, preserving headers."""
        try:
            if not os.path.exists(self.input_csv_path):
                print(f"🚨 Error: Input CSV file not found at '{self.input_csv_path}'")
                return None

            # Read the entire CSV, keeping the header
            df_input = pd.read_csv(self.input_csv_path, keep_default_na=False) # keep_default_na=False to treat empty strings as such

            if df_input.empty:
                 print(f"🚨 Error: Input CSV file '{self.input_csv_path}' is empty.")
                 return None

            # Check if the search term column index is valid
            if self.search_term_column_index >= len(df_input.columns):
                 print(f"🚨 Error: Search term column index ({self.search_term_column_index}) is out of bounds for input CSV with {len(df_input.columns)} columns.")
                 return None

            print(f"✅ Read {len(df_input)} rows (including header) from '{os.path.basename(self.input_csv_path)}'")
            return df_input

        except pd.errors.EmptyDataError:
             print(f"🚨 Error: Input CSV file '{self.input_csv_path}' is empty.")
             return None
        except FileNotFoundError: # Should be caught by os.path.exists, but keep for robustness
             print(f"🚨 Error: Input CSV file not found at '{self.input_csv_path}'")
             return None
        except Exception as e:
            print(f"🚨 An error occurred while reading the input CSV '{self.input_csv_path}': {e}")
            return None

    async def _find_exact_match_status(self, page, search_box, search_term, grid_selector):
        """
        Searches for the term and finds the status of the row with an exact
        (case-insensitive) title match. Returns status string or None.
        """
        print(f"  -> Searching grid for exact match: '{search_term}'")
        found_status = None
        try:
            # Clear previous search and type new term
            await search_box.fill("")
            await search_box.fill(search_term)
            # print(f"  ⏳ Waiting {self.search_wait / 1000}s for filter...")
            await page.wait_for_timeout(self.search_wait) # Wait for JS filtering

            # Re-fetch rows after search
            rows = await page.query_selector_all(f"{grid_selector} div[role='row']")
            # print(f"  📊 Found {len(rows)} rows potentially matching.")

            if not rows:
                print(f"  ❌ No rows found in grid after searching for '{search_term}'.")
                return None # No rows means no match

            for row in rows:
                cells = await row.query_selector_all("div[role='gridcell']")
                if len(cells) > self.web_title_cell_index:
                    try:
                        web_title = (await cells[self.web_title_cell_index].inner_text()).strip()

                        # --- Exact, Case-Insensitive Match ---
                        if web_title.lower() == search_term.lower():
                            print(f"  ✅ Found exact match: '{web_title}'")
                            # Extract Status from the row's class attribute
                            status = "N/A" # Default status
                            try:
                                class_attr = await row.get_attribute("class") or ""
                                classes = class_attr.split()
                                potential_status = classes[-1] if classes else ""
                                if potential_status.startswith("status-"):
                                     status = potential_status.replace("status-", "").capitalize()
                                elif len(classes) > 1: # Fallback if no 'status-' prefix
                                    status = classes[-1]
                            except Exception as class_error:
                                print(f"  ⚠️ Error extracting status from class for '{search_term}': {class_error}")

                            found_status = status
                            break # Stop searching once exact match is found
                        # else:
                            # print(f"  ℹ️ Row title '{web_title}' doesn't exactly match '{search_term}'.")

                    except Exception as cell_error:
                        print(f"  ⚠️ Error reading cell {self.web_title_cell_index} for '{search_term}': {cell_error}")
                        continue # Skip row if cell reading fails
                # else:
                    # print(f"  ⚠️ Row doesn't have enough cells ({len(cells)}) to check title index {self.web_title_cell_index}.")


            if found_status is None:
                print(f"  ❌ No exact match found for '{search_term}' in the filtered results.")

            return found_status # Will be None if no exact match was found

        except Exception as e:
            print(f"  🚨 An error occurred while searching for '{search_term}': {e}")
            return None # Return None on error to indicate failure for this term

    def _write_output_csv(self, output_data, output_headers):
        """Writes the collected data (list of lists) to the output CSV file."""
        if not output_data:
            print("ℹ️ No data was processed to write.")
            return False

        output_dir = os.path.dirname(self.output_csv_path)
        if output_dir: # Ensure directory exists only if it's not the current dir
             os.makedirs(output_dir, exist_ok=True)

        print(f"\n💾 Writing {len(output_data)} total rows to {self.output_csv_path}...")
        try:
            df_results = pd.DataFrame(output_data, columns=output_headers)
            df_results.to_csv(self.output_csv_path, mode='w', header=True, index=False, encoding='utf-8-sig')
            print(f"✅ Results saved successfully to '{self.output_csv_path}'")
            return True
        except Exception as e:
            print(f"🚨 An error occurred while writing the output CSV to '{self.output_csv_path}': {e}")
            return False


    async def run_scrape(self):
        """Main method to orchestrate the scraping process."""
        df_input = self._read_input_csv()
        if df_input is None:
            return False # Indicate failure if input couldn't be read

        # Prepare output headers (original headers + new column)
        output_headers = df_input.columns.tolist() + ["SteamOSResult"]
        output_data = [] # To store list of lists for output rows

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            url = "https://checkmydeck.ofdgn.com/all-games?sort=deck-compat-last-change-desc"
            try:
                await page.goto(url, timeout=60000)
                print(f"🚀 Browser opened to {url}. Please log in manually if required. Waiting {self.login_wait / 1000} seconds...")
                await page.wait_for_timeout(self.login_wait)

                grid_selector = "div[role='grid']"
                search_input_selector = "input[placeholder='Type to filter']"
                await page.wait_for_selector(grid_selector, timeout=30000)
                search_box_element = await page.wait_for_selector(search_input_selector, timeout=30000)
                print("✅ Grid and search bar loaded.")

            except Exception as e:
                print(f"🚨 Error during initial page load or finding elements: {e}")
                await browser.close()
                return False # Indicate failure

            if not search_box_element:
                print(f"🚨 Could not find the search input element.")
                await browser.close()
                return False # Indicate failure

            # --- Iterate through input DataFrame rows ---
            total_rows = len(df_input)
            for index, row in df_input.iterrows():
                search_term = str(row.iloc[self.search_term_column_index]).strip() # Get search term from configured column
                print(f"\nProcessing row {index + 1}/{total_rows}: '{search_term}'")

                if not search_term:
                    print("  ⚠️ Skipping row due to empty search term.")
                    status_result = "Skipped (Empty)"
                else:
                    # Find the status for the exact match
                    status_result = await self._find_exact_match_status(page, search_box_element, search_term, grid_selector)
                    if status_result is None:
                        status_result = "Not Found" # Use "Not Found" if scraping failed or no match

                # Construct the output row: original data + status result
                output_row = row.tolist() + [status_result]
                output_data.append(output_row)

                # Optional small delay between searches if needed to avoid rate limiting
                # await asyncio.sleep(0.2)

            await browser.close()
            print("\n🔒 Browser closed.")

        # Write the final results
        return self._write_output_csv(output_data, output_headers)

# Example of how to run this class (for testing)
# async def test_run():
#     # Make sure 'input_test.csv' exists in the same directory or provide full path
#     # And it has game titles in the first column (after a header row)
#     scraper = ListedGameScraper("input_test.csv", "output_test.csv")
#     success = await scraper.run_scrape()
#     if success:
#         print("Test scrape completed successfully.")
#     else:
#         print("Test scrape failed.")

# if __name__ == "__main__":
#     asyncio.run(test_run())