CREATE VIEW gold.dim_customer AS (
	SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE	
		WHEN cst_gndr != 'n/a' OR cst_gndr IS NOT NULL THEN cst_gndr
		else COALESCE(GEN, 'n/a')
	END AS gender,
	ca.BDATE AS birth_date,
	cl.CNTRY AS country,
	ci.cst_create_date AS create_date
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.CID
	LEFT JOIN silver.erp_loc_a101 AS cl
	ON ci.cst_key = cl.CID
);

CREATE VIEW gold.dim_products AS (
	SELECT
	ROW_NUMBER() OVER (ORDER BY prd_key,prd_start_dt) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	c.CAT AS category,
	c.SUBCAT AS sub_category,
	c.MAINTENANCE AS maintenance,
	p.prd_cost AS product_cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS product_start_date
	FROM silver.crm_prd_info AS p
	LEFT JOIN silver.erp_px_cat_g1v2 AS c
	ON p.cat_id = c.ID
	WHERE p.prd_end_dt IS NULL
);

CREATE VIEW gold.fact_sales AS (
	SELECT
	f.sls_ord_num,
	p.product_key,
	c.customer_key,
	f.sls_order_dt,
	f.sls_ship_dt,
	f.sls_due_dt,
	f.sls_sales,
	f.sls_quantity,
	f.sls_price
	FROM silver.crm_sales_details AS f
	LEFT JOIN gold.dim_customer AS c
	ON c.customer_id = f.sls_cust_id
	LEFT JOIN gold.dim_products AS p
	ON p.product_number = f.sls_prd_key
);
