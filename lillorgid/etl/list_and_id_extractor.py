import requests

class ListAndIdExtractor:


    def __init__(self):
        url = "https://org-id.guide/download.json"
        self.lists = []
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            data = r.json()
            self.lists = [i['code'].lower() for i in data['lists']]

    def process(self, list_and_id: str, seperator:str='-'):
        # Is this a known list?
        for l in self.lists:
            if list_and_id.lower().startswith(l + seperator):
                return (l, list_and_id[len(l)+1:], True)
        # We guess ....
        bits = list_and_id.split(seperator)
        id = bits.pop(-1)
        return (seperator.join(bits).lower(), id, False)
