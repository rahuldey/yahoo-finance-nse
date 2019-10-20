import time
from lxml import html
import requests
import yaml
import json
import pandas as pd


def read_configuration():
    with open('./xpaths.yml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


class Scraper(object):
    def __init__(self, report_type, xpaths, company, base_url):
        url = base_url.format(company, report_type)
        self.response = html.fromstring(requests.get(url).content)
        self.xpaths = xpaths
        self.company = company
        self.report_type = report_type

    def scraped_results(self):
        results = []
        index = 2
        columns = list(self.xpaths.keys())

        if 'date' not in columns:
            raise ValueError(f'Date not present in the configuration file for report type: {self.report_type}')
        columns.remove('date')

        while True:
            result = {}    
            date = self.response.xpath(self.xpaths['date'].format(index))
            if date == []:
                break
            
            result['company'] = self.company
            result['date'] = date
            result['report_type'] = self.report_type

            for key in self.xpaths.keys():
                ele = self.response.xpath(self.xpaths[key].format(index))
                result[key] = ele[0] if ele not in [[], None] else ''

            results.append(result)
            index += 1
        
        return results

            

def main():
    base_url = 'https://in.finance.yahoo.com/quote/{}.NS/{}'

    config = read_configuration()
    report_types = []
    results = []

    for report_type in config:
        if len(report_type) != 1:
            raise ValueError('Incorrect config file')
        report_types.append(list(report_type.keys())[0])

    companies_list = pd.read_excel('./NSE.xlsx').SYMBOL.tolist()
    
    for company in companies_list:
        for report_type in report_types:
            controller = Scraper(report_type=report_type,
            xpaths=list(filter(lambda x: list(x.keys())[0] == report_type, config))[0][report_type],
            company=company,
            base_url=base_url)

            for x in controller.scraped_results():
                results.append(x)
            time.sleep(1)

    print(json.dumps(results))

main()
