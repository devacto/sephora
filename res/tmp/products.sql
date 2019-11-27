SELECT
    p.id As productId,
    i.urls AS urls,
    c.categories AS categories
FROM `sephora-sde-test.raw.products` p
    LEFT JOIN `sephora-sde-test.tmp.product_images` i ON p.id = i.productId
    LEFT JOIN `sephora-sde-test.tmp.product_categories` c ON p.id = c.productId
