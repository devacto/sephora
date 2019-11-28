import yaml

with open(r'dags/dependencies.yml') as file:
    deps = yaml.load(file, Loader=yaml.FullLoader)
    roots = []
    leafs = []
    all_items = []

    all_files = []
    all_depends_on = []
    
    d = deps['dependencies']
    for item in d:
        if item['file'] not in all_items:
            all_items.append(item['file'])
        if item['depends_on'] not in all_items:
            all_items.append(item['depends_on'])
        if item['file'] not in all_files:
            all_files.append(item['file'])
        if item['depends_on'] not in all_depends_on:
            all_depends_on.append(item['depends_on'])

    roots = [item for item in all_depends_on if item not in all_files]
    leafs = [item for item in all_files if item not in all_depends_on]

    print('\n\nroots: {}\n\n\n'.format(str(roots)))
    # roots: ['tmp/item_purchase_prices.sql',
    #         'tmp/variant_images.sql',
    #         'tmp/inventory_items.sql',
    #         'tmp/product_images.sql',
    #         'tmp/product_categories.sql',
    #         'C']

    print('leafs: {}\n\n\n'.format(str(leafs)))
    # leafs: ['tmp/variants.sql', 'tmp/products.sql', 'A']