from urllib.request import urlopen
from bs4 import BeautifulSoup
import markdown


def fetch_content(url):

    response = urlopen(url)
    html = response.read().decode("utf-8")
    soup = BeautifulSoup(html, features="html.parser")

    # Try to find content within main or body elements
    content = ""
    main_content = soup.find("main")
    if main_content:
        content = "\n".join([tag.get_text(strip=True) for tag in main_content.find_all()])
    else:
        body_content = soup.find("body")
        if body_content:
            content = "\n".join([tag.get_text(strip=True) for tag in body_content.find_all()])

    return content


def main():

    user_input = input("Enter URLs separated by commas (or plain text): ")

    # Split input by commas, handling potential spaces
    urls = [url.strip() for url in user_input.split(",")]

    for url in urls:
        # Check if URL using a simple validation
        if url.startswith(("http://", "https://")):
            try:
                content = fetch_content(url)
                if content:

                    print(markdown.markdown(content))
                else:
                    print(f"No main or body element found in {url}")
            except Exception as e:
                print(f"Error fetching content for {url}: {e}")
        else:
            print(url)  


if __name__ == "__main__":
    main()