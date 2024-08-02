import time
import csv
import logging
import pandas as pd

from bs4 import BeautifulSoup

from include.utils.minio import get_minio_client
from include.utils.try_request import try_request


def scrap_all_notaries():
    base_url = 'https://notaries-directory.eu'
    search_url_template = f'{base_url}/pl/search?country_code=POL&page='
    raw_csv_file_path = 'raw_notaries.csv'
    bucket_name = 'notaries-bucket'
    raw_object_name = 'raw_notaries.csv'

    with open(raw_csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')

        writer.writerow(['Name', 'Address 1', 'Address 2', 'Address 3', 'Detail Link', 'Phone Number', 'Email'])

        page = 0
        while page < 10:
            search_url = search_url_template + str(page)

            response = try_request(search_url, 10)
            soup = BeautifulSoup(response.content, 'html.parser')

            notary_names = soup.find_all(class_='notary-name')
            addresses = soup.find_all(class_='address-notary')
            links = soup.find_all(class_='btn notary-detail-link')

            # Testing...
            if not notary_names:
                break

            for name, address, link in zip(notary_names, addresses, links):
                notary_name = name.get_text(strip=True)
                notary_address = address.get_text(strip=True)
                notary_link = base_url + link['href']

                detail_response = try_request(notary_link, 10)
                detail_soup = BeautifulSoup(detail_response.content, 'html.parser')

                # Extract phone number and email
                phone_number = detail_soup.find(class_='info-with-icon phone').get_text(strip=True)
                email = detail_soup.find(class_='info-with-icon link-redirect mail').get_text(strip=True)

                # Subtract address from name
                notary_name = notary_name.replace(notary_address, '').strip()

                address_parts = notary_address.split(',')
                address = address_parts[0].strip()
                city = address_parts[1].strip() if len(address_parts) > 1 else ''
                capital_city = address_parts[2].strip() if len(address_parts) > 2 else ''

                writer.writerow([notary_name, address, city, capital_city, notary_link, phone_number, email])
                print(f'Wrote raw data for {notary_name} to the CSV file.')

                time.sleep(3)

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

        # Split the notary name into parts and transform the data
        def split_name(name):
            name_parts = name.split()
            first_name = name_parts[0]
            last_name = name_parts[-1]
            secondary_name = name_parts[1] if len(name_parts) == 3 else None
            return pd.Series([first_name, secondary_name, last_name])

        df[['First Name', 'Secondary Name', 'Last Name']] = df['Name'].apply(split_name)
        df = df.rename(columns={'Address 1': 'Address', 'Address 2': 'City', 'Address 3': 'Capital City'})
        df = df[
            ['First Name', 'Secondary Name', 'Last Name', 'Address', 'City', 'Capital City', 'Phone Number', 'Email']]

        df.to_csv(transformed_csv_file_path, sep=';', index=False)
        print(f'Wrote transformed data to {transformed_csv_file_path}')

        client.fput_object(bucket_name, transformed_object_name, transformed_csv_file_path)
        print(f'Uploaded {transformed_csv_file_path} to Minio bucket {bucket_name} as {transformed_object_name}')
    except Exception as e:
        logging.error(f'Error processing file: {e}')
        raise
