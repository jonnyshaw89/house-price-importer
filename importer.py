import csv
import datetime
import json

import boto3 as boto3
from botocore.vendored import requests

s3_client = boto3.client('s3')

unique_id = 0,
price_paid = 1
deed_date = 2
postcode = 3
property_type = 4
new_build = 5
estate_type = 6
saon = 7
paon = 8
street = 9
locality = 10
town = 11
district = 12
county = 13
transaction_category = 14
linked_data_uri = 15

S3_BUCKET = 'lots-of-data'
S3_KEY_PREFIX = 'house_prices_json'

DATA_RANGE_YEAR_START = 1995

_DAYS_IN_MONTH = [-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

class PricePaid(object):
    def __init__(self, unique_id, price_paid, deed_date, postcode,
                 property_type, new_build, estate_type, saon, paon,
                 street, locality, town, district, county,
                 transaction_category, linked_data_uri):

        # self.unique_id = unique_id,
        self.price_paid = price_paid
        self.deed_date = deed_date
        self.postcode = postcode
        self.property_type = property_type
        self.new_build = new_build
        self.estate_type = estate_type
        self.saon = saon
        self.paon = paon
        self.street = street
        self.locality = locality
        self.town = town
        self.district = district
        self.county = county
        self.transaction_category = transaction_category
        # self.linked_data_uri = linked_data_uri



def import_price_paid_data(from_date, to_date, key_prefix):

    print('Importing for')
    print(from_date)
    print(to_date)
    print(key_prefix)

    url = "http://landregistry.data.gov.uk/app/ppd/ppd_data.csv?" \
          "et[]=lrcommon:freehold&" \
          "et[]=lrcommon:leasehold&" \
          "header=true&" \
          "limit=all&" \
          "min_date={}&" \
          "max_date={}&" \
          "nb[]=true&" \
          "nb[]=false&" \
          "ptype[]=lrcommon:detached&" \
          "ptype[]=lrcommon:semi-detached&" \
          "ptype[]=lrcommon:terraced&" \
          "ptype[]=lrcommon:flat-maisonette&" \
          "ptype[]=lrcommon:otherPropertyType&" \
          "tc[]=ppd:standardPricePaidTransaction&" \
          "tc[]=ppd:additionalPricePaidTransaction".format(from_date, to_date)

    r = requests.get(url)

    print("Status Code: " + str(r.status_code))

    csv_content = r.content.decode('utf-8')

    reader = csv.reader(csv_content.split('\n'), delimiter=',')

    line_count = 0
    for row in reader:
        if line_count == 0:
            line_count += 1
        else:
            if row:
                price_paid_json = PricePaid(
                    row[unique_id[0]],
                    int(row[price_paid]),
                    row[deed_date],
                    row[postcode],
                    row[property_type],
                    row[new_build],
                    row[estate_type],
                    row[saon],
                    row[paon],
                    row[street],
                    row[locality],
                    row[town],
                    row[district],
                    row[county],
                    row[transaction_category],
                    row[linked_data_uri],
                )

                json_output = json.dumps(vars(price_paid_json))
                # print(json_output)
                s3_client.put_object(Body=json_output, Bucket=S3_BUCKET,
                                  Key='{}/{}.json'.format(key_prefix, row[unique_id[0]]))

            line_count += 1

    print('Loaded Records: ' + (line_count-1))
    s3_client.put_object(Body='Loaded Records: ' + (line_count-1), Bucket=S3_BUCKET,
                         Key='{}/finished.txt'.format(key_prefix))


def import_data():
    now = datetime.datetime.now()

    for year in range(DATA_RANGE_YEAR_START, now.year):
        for month in range(1, 13):
            s3_prefix = '{}/{}/{}'.format(S3_KEY_PREFIX, year, datetime.date(year, month, 1).strftime('%B'))
            list_response = s3_client.list_objects_v2(Bucket=S3_BUCKET,
                                                      Prefix=s3_prefix + '/finished.txt')

            if list_response.get('KeyCount') == 0:
                import_price_paid_data(datetime.date(year, month, 1).strftime('%d %B %Y'),
                                       datetime.date(year, month, _DAYS_IN_MONTH[month]).strftime('%d %B %Y'),
                                       s3_prefix
                                       )

def lambda_handler(context, event):
    import_data()

if __name__ == '__main__':
    import_data()