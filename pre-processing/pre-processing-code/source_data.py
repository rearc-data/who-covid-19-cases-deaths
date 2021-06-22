import os
import boto3
import json
import csv
from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from s3_md5_compare import md5_compare

class MyHTMLParser(HTMLParser):

	def __init__(self):
		HTMLParser.__init__(self)
		self.data = None

	def handle_starttag(self, tag, attr):
		if tag.lower() == 'a' and self.data == None:
			for item in attr:
				if item[0].lower() == 'href' and item[1].endswith('csv'):
					self.data = item[1]

def source_dataset(): #(new_filename, s3_bucket, new_s3_key):
			
	source_url = 'https://data.humdata.org/dataset/coronavirus-covid-19-cases-and-deaths'

	data_set_name = os.environ['DATASET_NAME']
	filename = data_set_name
	new_s3_key = data_set_name + '/dataset/' # + filename
	s3_bucket = os.environ['ASSET_BUCKET']

	file_location = '/tmp/' + filename

	try:
		source_response = urlopen(source_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, filename)

	except URLError as e:
		raise Exception('URLError: ', e.reason, filename)

	else:


		html = source_response.read().decode()
		parser = MyHTMLParser()
		parser.feed(html)

		data_url = parser.data

		try:
			data_response = urlopen(data_url)

		except HTTPError as e:
			raise Exception('HTTPError: ', e.code, filename)

		except URLError as e:
			raise Exception('URLError: ', e.reason, filename)

		else:
			data = data_response.read()

			with open('/tmp/source.csv', 'wb') as s:
				s.write(data)

			with open('/tmp/source.csv', 'r') as s, open('/tmp/' + filename + '.csv', 'w', encoding='utf-8') as c:
				reader = csv.DictReader(s)
				writer = csv.DictWriter(c, fieldnames=reader.fieldnames)
				writer.writeheader()

				for row in reader:
					writer.writerow(row)

			with open('/tmp/source.csv', 'r') as s, open('/tmp/' + filename + '.json', 'w', encoding='utf-8') as j:
				reader = csv.DictReader(s)
				j.write('\n'.join(json.dumps(row, ensure_ascii=False)
								  for row in reader))
			
			os.remove('/tmp/source.csv')

			# Creates S3 connection
			s3 = boto3.client('s3')

			asset_list = []

			# Looping through filenames, uploading to S3
			for filename in os.listdir('/tmp'):
				if filename.startswith(data_set_name):
					#s3.upload_file('/tmp/' + filename, s3_bucket,
					#		new_s3_key + filename)
					file_location = '/tmp/' + filename
					has_changes = md5_compare(s3, s3_bucket, new_s3_key + filename, file_location)
					if has_changes:
						s3.upload_file(file_location, s3_bucket, new_s3_key + filename)
						asset_list.append(
							{'Bucket': s3_bucket, 'Key': new_s3_key + filename})
						print('Uploaded: ' + filename)
					else:
						print('No changes in: ' + filename)



			return asset_list

if __name__ == '__main__':
	asset_list = source_dataset()
	print(asset_list)