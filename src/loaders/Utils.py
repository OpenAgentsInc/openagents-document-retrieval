from urllib.request import Request, urlopen
import traceback
class Utils:
    @staticmethod
    def existsUrl(url):
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            conn = urlopen(req)
            conn.close()
            return True
        except:
            return False

    @staticmethod
    def fetch(url, expectedMimes = None, binary=False):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'}
            req = Request(url, headers=headers)
            conn = urlopen(req)
            if expectedMimes:
                mimetype = conn.getheader('Content-Type')
                if mimetype:
                    mimetype = mimetype.split(";")[0]
                if not mimetype in expectedMimes:
                    print("Mime type not expected",mimetype)
                    conn.close()
                    return None
            content = conn.read()
            if not binary:
                content = content.decode("utf-8")
            conn.close()
            return content
        except Exception as e:
            print("Error fetching url",url,"error",e)
            traceback.print_exc()
            return None