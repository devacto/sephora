SELECT
    pr.id As productId,
    ARRAY_AGG(
        p.url
    ) AS urls
FROM `sephora-sde-test.raw.products` pr
  LEFT JOIN `sephora-sde-test.raw.pictures` p ON pr.external_id = p.external_id
WHERE p.type = 'product'
GROUP BY 1