import http.client
from urllib.parse import urljoin, urlparse

import duckdb
from bs4 import BeautifulSoup

from address_matching.token_and_term_frequencies import (
    get_address_token_frequencies_from_address_table,
    get_numeric_term_frequencies_from_address_table,
)

con = duckdb.connect(database=":memory:")


def get_psc_snapshot_links(url):

    parsed_url = urlparse(url)
    conn = http.client.HTTPSConnection(parsed_url.netloc)

    conn.request("GET", parsed_url.path)
    response = conn.getresponse()

    if response.status != 200:
        raise Exception(f"Request failed with status: {response.status}")

    html_content = response.read()
    soup = BeautifulSoup(html_content, "html.parser")

    li_elements = soup.find_all("li")

    links = []
    for li in li_elements:
        a_tag = li.find("a")
        if a_tag and "of" in a_tag.text:
            full_link = urljoin(url, a_tag["href"])
            links.append(full_link)

    return links


url = "https://download.companieshouse.gov.uk/en_pscdata.html"


psc_links = get_psc_snapshot_links(url)


for index, link in enumerate(psc_links, start=1):
    print(f"{index}. {link}")


path = "/Users/robinlinacre/Downloads/psc-snapshot-2024-06-24_1of27.txt"


sql = f"""
with addresses as (
SELECT
    row_number() over()  as unique_id,
    company_number,
    data -> 'address' ->> 'premises' AS premises,
    data -> 'address' ->> 'address_line_1' AS address_line_1,
    data -> 'address' ->> 'address_line_2' AS address_line_2,
    data -> 'address' ->> 'country' AS country,
    data -> 'address' ->> 'locality' AS locality,
    data -> 'address' ->> 'postal_code' AS postal_code,
    data -> 'address' ->> 'region' AS region
FROM read_json_auto('{path}')
where country in (
'England',
'United Kingdom',
'Scotland',
'Wales',
'Northern Ireland',
'Great Britain'
) or country is null
)
select
unique_id,
concat_ws(' ', premises, address_line_1, address_line_2, locality, region) as address_concat,
postal_code as postcode
from addresses


limit 1000

"""
address_table = con.sql(sql)


tf_address_tokens = get_address_token_frequencies_from_address_table(
    address_table, con=con
)
tf_num_tokens = get_numeric_term_frequencies_from_address_table(address_table, con=con)
