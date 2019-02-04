import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer

def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London gva'
    package.pkg.descriptor['name'] = 'london-gva'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/gva.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'gva'

    package.pkg.descriptor['resources'][1]['path'] = 'data/gva-per-capita.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'gva-per-capita'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield from package

def filter_gva(rows):
    for row in rows:
        if row['1997'] is not '':
            yield row

def london_gva(link):
    Flow(
        load(link,
             sheet=2),
        load(link,
             sheet=3),
        filter_gva,
        set_format_and_name,
        dump_to_path('data'),
        printer(num_rows=1)
    ).process()


london_gva('https://data.london.gov.uk/download/gross-value-added-and-gross-disposable-household-income/922c4abe-d75e-4c58-b6ed-9dace9f933a3/GVA-GDHI-nuts3-regions-uk.xls')