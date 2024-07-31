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
    csv_file_path = 'notaries.csv'
    bucket_name = 'notaries-bucket'
    object_name = 'notaries.csv'

    # Open CSV file in write mode with delimiter ;
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        # Write the header
        writer.writerow(
            ['First Name', 'Secondary Name', 'Last Name', 'Address', 'City', 'Capital City', 'Phone Number', 'Email'])

        page = 0
        while page <= 4:
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
                notary_name = notary_name.replace(notary_address, '').strip()  # Subtract address from name

                # Split the notary name into parts
                name_parts = notary_name.split()
                first_name = name_parts[0]
                last_name = name_parts[-1]
                secondary_name = name_parts[1] if len(name_parts) == 3 else None

                # Split the address into parts
                address_parts = notary_address.split(',')
                address = address_parts[0].strip()
                city = address_parts[1].strip() if len(address_parts) > 1 else ''
                capital_city = address_parts[2].strip() if len(address_parts) > 2 else ''

                # Write the notary details to the CSV file
                writer.writerow(
                    [first_name, secondary_name, last_name, address, city, capital_city, phone_number, email])
                print(f'Wrote {first_name} {last_name} to the CSV file.')

            page += 1
            time.sleep(2)

            # Add a delay to avoid making too many requests in a short period
            time.sleep(5)

    # Upload the CSV file to Minio
    try:
        client = get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f'Created bucket {bucket_name}')
        client.fput_object(bucket_name, object_name, csv_file_path)
        print(f'Uploaded {csv_file_path} to Minio bucket {bucket_name} as {object_name}')
    except Exception as e:
        logging.error(f'Error uploading file to Minio: {e}')
