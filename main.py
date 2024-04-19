import requests
from bs4 import BeautifulSoup

def convert_html_to_markdown(text):
  """Converts basic HTML tags to markdown formatting with link preservation"""
  soup = BeautifulSoup(text, 'html.parser')

  # Replacements for basic markdown elements with tag preservation
  replacements = {
      "<h1>": "## {text.text.strip()}\n",
      "<h2>": "### {text.text.strip()}\n",
      "<h3>": "#### {text.text.strip()}\n",
      "<b>": "**{text.text.strip()}**",
      "</b>": "",
      "<strong>": "**{text.text.strip()}**",
      "</strong>": "",
      "<i>": "*{text.text.strip()}*",
      "<i>": "",
  }

  for tag, replacement in replacements.items():
    for element in soup.find_all(tag):
      if tag == "a":  # Special handling for anchor tags with links
        link_text = element.text.strip()
        link_href = element.get("href")
        new_element = f"{replacement.format(text=link_text)} [{link_text}]({link_href})"
      else:
        new_element = replacement.format(text=element)
      element.replace_with(new_element)
  return soup.get_text().strip()

def convert_to_markdown_lite(text):
  """Applies basic markdown formatting (bold, italics, headers) - for plain text"""
  # (unchanged from original script)
  replacements = {
      "<h1>": "## ",
      "<h2>": "### ",
      "<h3>": "#### ",
      "<b>": "**",  # Bold
      "</b>": "**",
      "<strong>": "**",
      "</strong>": "**",
      "<i>": "*",  # Italics
      "<i>": "*",
  }
  for before, after in replacements.items():
    text = text.replace(before, after)
  return text.strip()

def process_input(user_input):
  """Processes user input based on type (text or URL)"""
  if isinstance(user_input, str):
    # Check if it's a URL using a simple validation
    if user_input.startswith("http"):
      try:
        response = requests.get(user_input)
        response.raise_for_status()  # Raise error for non-2xx status codes
        content = convert_html_to_markdown(response.text)
        print(f"**Content from URL:**\n{content}")
      except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
    else:
      # Plain text, convert to markdown lite
      content = convert_to_markdown_lite(user_input)
      print(content)
  else:
    # Handle list of URLs
    for url in user_input:
      process_input(url)

# Get user input
user_input = input("Enter URL(s) or text (separate URLs with commas): ")

# Split input into a list if it contains commas
if "," in user_input:
  user_input = user_input.split(",")

# Process each input
process_input(user_input)