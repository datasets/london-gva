import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer, unpivot


def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London gva'
    package.pkg.descriptor['name'] = 'london-gva'
    # Change path and name for the resource:

    package.pkg.descriptor['resources'][0]['path'] = 'data/gva-london.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'london-gva'

    package.pkg.descriptor['resources'][1]['path'] = 'data/gva.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'gva'



    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield from package


def filter_gva_london(rows):
    for row in rows:
        if row['NUTS code'] == 'UKI':
            yield row

def filter_gva(rows):
    for row in rows:
        if row['NUTS code'] is not '':
            yield row

unpivot_fields = [
    {'name': '1997', 'keys': {'Value': '1997', 'Year': '1997'}},
    {'name': '1998', 'keys': {'Value': '1998', 'Year': '1998'}},
    {'name': '1999', 'keys': {'Value': '1999', 'Year': '1999'}},
    {'name': '2000', 'keys': {'Value': '2000', 'Year': '2000'}},
    {'name': '2001', 'keys': {'Value': '2001', 'Year': '2001'}},
    {'name': '2002', 'keys': {'Value': '2002', 'Year': '2002'}},
    {'name': '2003', 'keys': {'Value': '2003', 'Year': '2003'}},
    {'name': '2004', 'keys': {'Value': '2004', 'Year': '2004'}},
    {'name': '2005', 'keys': {'Value': '2005', 'Year': '2005'}},
    {'name': '2006', 'keys': {'Value': '2006', 'Year': '2006'}},
    {'name': '2007', 'keys': {'Value': '2007', 'Year': '2007'}},
    {'name': '2008', 'keys': {'Value': '2008', 'Year': '2008'}},
    {'name': '2009', 'keys': {'Value': '2009', 'Year': '2009'}},
    {'name': '2010', 'keys': {'Value': '2010', 'Year': '2010'}},
    {'name': '2011', 'keys': {'Value': '2011', 'Year': '2011'}},
    {'name': '2012', 'keys': {'Value': '2012', 'Year': '2012'}},
    {'name': '2013', 'keys': {'Value': '2013', 'Year': '2013'}},
    {'name': '2014', 'keys': {'Value': '2014', 'Year': '2014'}}
]

extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}

def london_gva(link):
    Flow(
        load(link,
             sheet=2),
        filter_gva_london,
        load(link,
             sheet=2),
        filter_gva,
        unpivot(unpivot_fields, extra_keys, extra_value),


        set_format_and_name,
        dump_to_path(),
        printer(num_rows=1)
    ).process()



london_gva('https://data.london.gov.uk/download/gross-value-added-and-gross-disposable-household-income/922c4abe-d75e-4c58-b6ed-9dace9f933a3/GVA-GDHI-nuts3-regions-uk.xls')