import os
import boto3
import json
import csv
from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

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
			
	source_url = 'https://data.humdata.org/dataset/coronavirus-covid-19-cases-and-deaths'
	
	try:
		source_response = urlopen(source_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, new_filename)

	except URLError as e:
		raise Exception('URLError: ', e.reason, new_filename)

	else:

		html = source_response.read().decode()
		parser = MyHTMLParser()
		parser.feed(html)

		data_url = parser.data

		try:
			data_response = urlopen(data_url)

		except HTTPError as e:
			raise Exception('HTTPError: ', e.code, new_filename)

		except URLError as e:
			raise Exception('URLError: ', e.reason, new_filename)

		else:
			data = data_response.read()

			with open('/tmp/source.csv', 'wb') as s:
				s.write(data)

			with open('/tmp/source.csv', 'r') as s, open('/tmp/' + new_filename + '.csv', 'w', encoding='utf-8') as c:
				reader = csv.DictReader(s)
				writer = csv.DictWriter(c, fieldnames=reader.fieldnames)
				writer.writeheader()

				for row in reader:
					writer.writerow(row)

			with open('/tmp/source.csv', 'r') as s, open('/tmp/' + new_filename + '.json', 'w', encoding='utf-8') as j:
				reader = csv.DictReader(s)
				j.write('\n'.join(json.dumps(row, ensure_ascii=False)
								  for row in reader))
			
			os.remove('/tmp/source.csv')

			asset_list = []

			# Creates S3 connection
			s3 = boto3.client('s3')

			# Looping through filenames, uploading to S3
			for filename in os.listdir('/tmp'):

				s3.upload_file('/tmp/' + filename, s3_bucket,
							new_s3_key + filename)

				asset_list.append(
					{'Bucket': s3_bucket, 'Key': new_s3_key + filename})

			return asset_list
