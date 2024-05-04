from bs4 import BeautifulSoup
from markdownify import markdownify
from loaders.Loader import Loader
from io import BytesIO
import PyPDF2
import time
from loaders.Utils import Utils

class PDFLoader(Loader):
    def __init__(self,runner):
        super().__init__(runner)

    

    def load(self,url):
        content = Utils.fetch(url,["application/pdf"], True)
        if not content:return [None,None]

        # Open the PDF file in read binary mode
        f = BytesIO(content)
        pdf_reader = PyPDF2.PdfReader(f)
        num_pages = len(pdf_reader.pages)
        # Iterate through all pages and extract text
        fullText = ""
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            fullText += text.strip()
        fullText+="\nSource: "+url+'\n\n'
        return [fullText], (time.time()+60*60*24*30)*1000