import os
import boto3
import json
import csv
from html.parser import HTMLParser
from urllib.request import urlopen


class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.data = None

    def handle_starttag(self, tag, attr):
        if tag.lower() == 'a' and self.data == None:
            for item in attr:
                if item[0].lower() == 'href' and item[1].endswith('csv'):
                    self.data = item[1]



def source_dataset(new_filename, s3_bucket, new_s3_key):
            
    url = urlopen('https://data.humdata.org/dataset/coronavirus-covid-19-cases-and-deaths').read().decode()
    parser = MyHTMLParser()
    parser.feed(url)

    file = urlopen(parser.data).read()

    with open('/tmp/source.csv', 'wb') as s:
        s.write(file)

    with open('/tmp/source.csv', 'r') as s, open('/tmp/' + new_filename + '.csv', 'w', encoding='utf-8') as c:
        reader = csv.DictReader(s)
        writer = csv.DictWriter(c, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            writer.writerow(row)

        print(reader.fieldnames)
    
    with open('/tmp/source.csv', 'r') as s, open('/tmp/' + new_filename + '.json', 'w', encoding='utf-8') as j:
        reader = csv.DictReader(s)
        j.write('\n'.join(json.dumps(row, ensure_ascii=False)
                          for row in reader))
        print(reader.fieldnames)

    os.remove('/tmp/source.csv')

    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + new_filename + '.json',
                   s3_bucket, new_s3_key + '.json')
    s3.upload_file('/tmp/' + new_filename + '.csv',
                   s3_bucket, new_s3_key + '.csv')