from bs4 import BeautifulSoup
from markdownify import markdownify
from urllib.request import Request, urlopen


def parsePDF(conn):
  pass

def parseHTML(conn):
  content = conn.read().decode("utf-8")
  soup = BeautifulSoup(content, features="html.parser")
  
  html = ""
  main_content = soup.find("main")
  if main_content:
      main_content_html=main_content.prettify()
      html += main_content_html
  else:
      body_content = soup.find("body")
      if body_content:
          body_content_html=body_content.prettify()
          html += body_content_html


  content = markdownify(html)
  return content

def fetch_content(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}
    req = Request(url, headers=headers)
    conn = urlopen(req)
    mimetype=conn.getheader('Content-Type')
    if "application/pdf" in mimetype:
      return parsePDF(conn)
    else:
      return parseHTML(conn)    


def main():

    user_input = input("Enter URLs separated by commas (or plain text): ")

    # Split input by commas, handling potential spaces
    inputData = [url.strip() for url in user_input.split(",")]
    outputData = []

    for url in inputData:  
      try:
          content = fetch_content(url)
          if content:
              print(f"\n**URL:** {url}")
              print(f"**Content (markdown):**\n{content}")
          else:
              print(f"No main or body element found in {url}")
      except Exception as e:
          print(f"Error fetching content for {url}: {e}")
      


if __name__ == "__main__":
    main()