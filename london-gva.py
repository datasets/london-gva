import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer, unpivot


def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London Gross Value Added (GVA)'
    package.pkg.descriptor['name'] = 'gva'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/gva.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'gva'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    yield first.it
    yield from package

def filter_gva(rows):
    for row in rows:
        if row['NUTS code'] is not '':
            yield row

def remove_duplicates(rows):
    seen = set()
    for row in rows:
        line = ''.join('{}{}'.format(key, val) for key, val in row.items())
        if line in seen: continue
        seen.add(line)
        yield row

unpivoting_fields = [{'name': '([0-9]{4})([0-9]{0,1})', 'keys': {'Year': '01-01-' r'\1'}}]

extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}


def london_gva(link):
    Flow(
        load(link,
             sheet=3),
        filter_gva,
        unpivot(unpivoting_fields, extra_keys, extra_value),
        remove_duplicates,
        set_format_and_name,
        dump_to_path(),
        printer(num_rows=1)
    ).process()



london_gva('https://data.london.gov.uk/download/gross-value-added-and-gross-disposable-household-income/922c4abe-d75e-4c58-b6ed-9dace9f933a3/GVA-GDHI-nuts3-regions-uk.xls')
