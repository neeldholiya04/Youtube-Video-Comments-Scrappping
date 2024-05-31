import logging
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import concurrent.futures
import random
from selenium.webdriver.chrome.options import Options

# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to scroll and load all comments
def load_all_comments(driver):
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(2)  # Wait to load the comments
        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Function to click on "Show more" buttons to load additional comments
def click_show_more_buttons(driver):
    show_more_buttons = driver.find_elements(By.XPATH, '//*[@id="more-replies"]')
    for button in show_more_buttons:
        try:
            button.click()
            time.sleep(2)  # Wait for comments to load
        except Exception as e:
            logging.error(f"Could not click on 'Show more' button: {e}")
            logging.error(traceback.format_exc())

# Function to scrape comments from a YouTube video
def get_youtube_comments(video_url):
    # Configure Chrome options for headless browsing
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(video_url)

    comments = []
    video_title = driver.title

    try:
        # Load all comments
        load_all_comments(driver)

        # Click on "Show more" buttons to load additional comments
        click_show_more_buttons(driver)

        # Wait for the comment section to fully load
        time.sleep(5)

        # Extract comments
        comment_elems = driver.find_elements(By.XPATH, '//*[@id="content-text"]')
        author_elems = driver.find_elements(By.XPATH, '//*[@id="author-text"]')

        for author_elem, comment_elem in zip(author_elems, comment_elems):
            author = author_elem.text.strip()
            comment = comment_elem.text.strip()
            comments.append({'video_url': video_url, 'video_title': video_title, 'author': author, 'comment': comment})

    except Exception as e:
        logging.error(f"Error occurred while scraping comments for video {video_url}: {e}")
        logging.error(traceback.format_exc())
        return None  # Indicate failure for this URL

    finally:
        driver.quit()

    return comments

# Function to implement rate limiting
def rate_limit(max_requests_per_minute=10):
    time.sleep(60 / max_requests_per_minute)

# Function to scrape comments from a list of YouTube video URLs 
def scrape_comments_from_urls(video_urls):
    all_comments = []

    # Use multi-threading to fetch comments from multiple videos concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(get_youtube_comments, video_urls)

        for result in results:
            if result:  # Check if comments were successfully retrieved
                all_comments.extend(result)

    return all_comments

# Main scraping logic
def scrape_comments(video_urls, file_path="youtube_comments.csv"):
    all_comments = scrape_comments_from_urls(video_urls)

    # Save comments to a CSV file
    if all_comments:
        comments_df = pd.DataFrame(all_comments)
        comments_df.to_csv(file_path, index=False)

        logging.info(f"Saved {len(all_comments)} comments to {file_path}")
    else:
        logging.info("No comments were successfully retrieved.")


# Example usage:
# 1. Provide video URLs directly
video_urls = [
    'https://www.youtube.com/watch?v=havARbP7Fyk'
]

# 2. Scrape comments with rate limiting (optional)
for url in video_urls:
    scrape_comments([url], file_path=f"{url.split('=')[1]}.csv")
    rate_limit(max_requests_per_minute=10)

# 3. Scrape comments from all URLs at once (without rate limiting)
scrape_comments(video_urls)