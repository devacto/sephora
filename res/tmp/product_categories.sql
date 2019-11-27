SELECT
    p.id As productId,
    ARRAY_AGG(
        STRUCT(
            c.category_name AS categoryName
        )
    ) AS categories
FROM `sephora-sde-test.raw.products` p
    LEFT JOIN `sephora-sde-test.raw.categories` c ON  p.category_id = c.id
GROUP BY 1