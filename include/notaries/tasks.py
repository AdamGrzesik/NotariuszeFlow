import time
import csv
import logging
import pandas as pd

from bs4 import BeautifulSoup

from include.utils.minio import get_minio_client
from include.utils.transform import split_name, split_address
from include.utils.try_request import try_request


def scrap_all_notaries():
    base_url = 'https://notaries-directory.eu'
    search_url_template = f'{base_url}/pl/search?country_code=POL&page='
    raw_csv_file_path = 'raw_notaries.csv'
    bucket_name = 'notaries-bucket'
    raw_object_name = 'raw_notaries.csv'

    with open(raw_csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')

        writer.writerow(['Name', 'Address', 'Detail Link', 'Phone Number', 'Fax Number', 'Email'])

        page = 0
        while True:
            search_url = search_url_template + str(page)

            response = try_request(search_url, 10)
            soup = BeautifulSoup(response.content, 'html.parser')

            links = soup.find_all(class_='btn notary-detail-link')

            if not links:
                break

            for link in links:
                notary_link = base_url + link['href']

                detail_response = try_request(notary_link, 10)
                detail_soup = BeautifulSoup(detail_response.content, 'html.parser')

                # Extract info
                notary_name = detail_soup.find(class_='title').get_text().replace('\n', ' ').strip()
                notary_address = detail_soup.find(class_='info-with-icon address').get_text().replace('<br>',
                                                                                                      ' ').strip()
                phone_number = detail_soup.find(class_='info-with-icon phone')
                phone_number = phone_number.get_text(strip=True) if phone_number else None
                fax_number = detail_soup.find(class_='info-with-icon fax')
                fax_number = fax_number.get_text(strip=True) if fax_number else None
                email = detail_soup.find(class_='info-with-icon link-redirect mail')
                email = email.get_text(strip=True) if email else None

                writer.writerow([notary_name, notary_address, notary_link, phone_number, fax_number, email])
                print(f'Wrote raw data for {notary_name} to the CSV file.')

                time.sleep(3)
            print(page)
            page += 1

            if page % 100 == 0:
                print('Waiting to prevent too many requests...')
                time.sleep(300)

    try:
        client = get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Created bucket {bucket_name}')
        client.fput_object(bucket_name, raw_object_name, raw_csv_file_path)
        print(f'Uploaded {raw_csv_file_path} to Minio bucket {bucket_name} as {raw_object_name}')
    except Exception as e:
        logging.error(f'Error uploading file to Minio: {e}')
        raise


def transform_notaries_data():
    bucket_name = 'notaries-bucket'
    raw_object_name = 'raw_notaries.csv'
    transformed_csv_file_path = 'transformed_notaries.csv'
    transformed_object_name = 'transformed_notaries.csv'

    try:
        client = get_minio_client()
        client.fget_object(bucket_name, raw_object_name, transformed_csv_file_path)
        print(f'Downloaded {raw_object_name} from Minio bucket {bucket_name}')

        df = pd.read_csv(transformed_csv_file_path, delimiter=';')

        df[['First Name', 'Secondary Name', 'Last Name']] = df['Name'].apply(split_name)
        df[['Address', 'Zip Code', 'City', 'Regional Capital']] = df['Address'].apply(split_address)
        df = df[['First Name', 'Secondary Name', 'Last Name', 'Address', 'Zip Code', 'City', 'Regional Capital',
                 'Phone Number', 'Fax Number', 'Email']]

        df.to_csv(transformed_csv_file_path, sep=';', index=False)
        print(f'Wrote transformed data to {transformed_csv_file_path}')

        client.fput_object(bucket_name, transformed_object_name, transformed_csv_file_path)
        print(f'Uploaded {transformed_csv_file_path} to Minio bucket {bucket_name} as {transformed_object_name}')
    except Exception as e:
        logging.error(f'Error processing file: {e}')
        raise
