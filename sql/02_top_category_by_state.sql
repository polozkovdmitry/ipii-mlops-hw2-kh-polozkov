-- Для каждого штата: категория (cat_id), в которой была самая большая транзакция.
SELECT
    us_state,
    argMax(cat_id, amount) AS top_cat_id,
    max(amount)            AS max_amount
FROM hw2.transactions
GROUP BY us_state
ORDER BY us_state;
