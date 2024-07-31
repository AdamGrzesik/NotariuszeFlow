import requests
from airflow.hooks.base import BaseHook
from bs4 import BeautifulSoup
from minio import Minio
import time
import csv
import logging


def get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def scrap_all_notaries():
    base_url = 'https://notaries-directory.eu'
    search_url_template = f'{base_url}/pl/search?country_code=POL&page='
    raw_csv_file_path = 'raw_notaries.csv'
    bucket_name = 'notaries-bucket'
    raw_object_name = 'raw_notaries.csv'

    # Open CSV file in write mode with delimiter ;
    with open(raw_csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        # Write the header
        writer.writerow(['Name', 'Address 1', 'Address 2', 'Address 3', 'Detail Link', 'Phone Number', 'Email'])

        page = 0
        while page <= 564:
            search_url = search_url_template + str(page)
            response = requests.get(search_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            notary_names = soup.find_all(class_='notary-name')
            addresses = soup.find_all(class_='address-notary')
            links = soup.find_all(class_='btn notary-detail-link')

            if not notary_names:
                break

            for name, address, link in zip(notary_names, addresses, links):
                notary_name = name.get_text(strip=True)
                notary_address = address.get_text(strip=True)
                notary_link = base_url + link['href']

                # Fetch the notary's detail page
                detail_response = requests.get(notary_link)
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

                # Write the raw notary details to the CSV file
                writer.writerow([notary_name, address, city, capital_city, notary_link, phone_number, email])
                print(f'Wrote raw data for {notary_name} to the CSV file.')

            time.sleep(10)
            page += 1

    try:
        client = get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Created bucket {bucket_name}')
        client.fput_object(bucket_name, raw_object_name, raw_csv_file_path)
        print(f'Uploaded {raw_csv_file_path} to Minio bucket {bucket_name} as {raw_object_name}')
    except Exception as e:
        logging.error(f'Error uploading file to Minio: {e}')


def transform_notaries_data():
    bucket_name = 'notaries-bucket'
    raw_object_name = 'raw_notaries.csv'
    transformed_csv_file_path = 'transformed_notaries.csv'
    transformed_object_name = 'transformed_notaries.csv'

    try:
        client = get_minio_client()
        client.fget_object(bucket_name, raw_object_name, transformed_csv_file_path)
        print(f'Downloaded {raw_object_name} from Minio bucket {bucket_name}')

        with open(transformed_csv_file_path, mode='r', newline='', encoding='utf-8') as raw_file:
            reader = csv.reader(raw_file, delimiter=';')
            header = next(reader)

            with open(transformed_csv_file_path, mode='w', newline='', encoding='utf-8') as transformed_file:
                writer = csv.writer(transformed_file, delimiter=';')
                writer.writerow(
                    ['First Name', 'Secondary Name', 'Last Name', 'Address', 'City', 'Capital City', 'Phone Number',
                     'Email'])

                for row in reader:
                    if len(row) != 7:
                        logging.error(f'Skipping row with unexpected number of columns: {row}')
                        continue

                    notary_name, address, city, capital_city, notary_link, phone_number, email = row

                    # Split the notary name into parts
                    name_parts = notary_name.split()
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                    secondary_name = name_parts[1] if len(name_parts) == 3 else None

                    # Write the transformed notary details to the CSV file
                    writer.writerow(
                        [first_name, secondary_name, last_name, address, city, capital_city, phone_number, email])
                    print(f'Wrote transformed data for {first_name} {last_name} to the CSV file.')

        # Upload the transformed CSV file to Minio
        client.fput_object(bucket_name, transformed_object_name, transformed_csv_file_path)
        print(f'Uploaded {transformed_csv_file_path} to Minio bucket {bucket_name} as {transformed_object_name}')
    except Exception as e:
        logging.error(f'Error processing file: {e}')