import re

import pandas as pd


def split_name(name):
    name_parts = name.split()
    first_name = name_parts[0]
    last_name = name_parts[-1]
    secondary_name = name_parts[1] if len(name_parts) == 3 else None
    return pd.Series([first_name, secondary_name, last_name])


def split_address(address):
    address = re.sub(r'\s{2,}', ',', address)
    address_parts = [part for part in address.split(',') if part]
    print(address_parts)
    address_1 = address_parts[0] if len(address_parts) > 0 else None
    address_2 = address_parts[1] if len(address_parts) > 1 else None
    address_3 = address_parts[2] if len(address_parts) > 2 else None
    address_4 = address_parts[3] if len(address_parts) > 3 else None
    return pd.Series([address_1, address_2, address_3, address_4])
