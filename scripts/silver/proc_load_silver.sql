/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
     BEGIN TRY
          SET @batch_start_time = GETDATE();
          PRINT '==================================='
          PRINT('Loading Silver Layer')
          PRINT '==================================='

          PRINT '-----------------------------------';
          PRINT 'Loading CRM Tables'
          PRINT '-----------------------------------';

          SET @start_time = GETDATE();
     -- ============================================================================
     -- Inserting Cleaned data from bronzze into Silver in silver.crm_cust_info table
     -- ============================================================================

          PRINT '>> Truncating Table: silver.crm_cust_info'
          TRUNCATE TABLE silver.crm_cust_info;
          PRINT '>> Inserting Data Into: silver.crm_cust_info'
          INSERT INTO silver.crm_cust_info (
          cst_id,
          cst_key,
          cst_firstname,
          cst_lastname,
          cst_marital_status,
          cst_gndr,
          cst_create_date
          )
          SELECT
          cst_id,
          cst_key,
          TRIM(cst_firstname) AS cst_firstname,
          TRIM(cst_lastname) AS cst_lastname,
          CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
               WHEN UPPER(cst_marital_status) = 'M' THen 'Married'
               ELSE 'n/a'
          END cst_marital_status,
          CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
               WHEN UPPER(cst_gndr) = 'M' THen 'Male'
               ELSE 'n/a'
          END cst_gndr,
          cst_create_date
          FROM(
          SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
          FROM bronze.crm_cust_info
          WHERE cst_id IS NOT NULL
          )t WHERE flag_last = 1
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';

          -- ============================================================================
          --Inserting Cleaned data from bronzze into Silver in silver.crm_prd_info table
          -- ============================================================================
          SET @start_time = GETDATE();
          PRINT '>> Truncating Table: silver.crm_prd_info'
          TRUNCATE TABLE silver.crm_prd_info;
          PRINT '>> Inserting Data Into: silver.crm_prd_info'
          INSERT INTO silver.crm_prd_info(
               prd_id,
               cat_id,
               prd_key,
               prd_nm,
               prd_cost,
               prd_line,
               prd_start_dt,
               prd_end_dt
          )
          SELECT 
               prd_id,
               REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')AS cat_id, -- Extract category ID (derivating a new col)
               SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product Key
               prd_nm,
               ISNULL (prd_cost, 0) AS prd_cost,
               CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
               END prd_line, -- Map product line codes to descriptie values (data Normalisation)
               CAST(prd_start_dt AS DATE) AS prd_start_dt, -- datatype casting
               CAST(
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
                    AS DATE
                    ) AS prd_end_dt --Calculate end date as one day before the next start date(data type casting)
          FROM [DataWarehouse].[bronze].[crm_prd_info]
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';


          -- ============================================================================
          --Inserting Cleaned data from bronzze into Silver in silver.crm_sales_details table
          -- ============================================================================

          SET @start_time = GETDATE();
          PRINT '>> Truncating Table: silver.crm_sales_details'
          TRUNCATE TABLE silver.crm_sales_details;
          PRINT '>> Inserting Data Into: silver.crm_sales_details'
          INSERT INTO silver.crm_sales_details(
               sls_ord_num,
               sls_prd_key,
               sls_cust_id,
               sls_order_dt,
               sls_ship_dt,
               sls_due_dt,
               sls_sales,
               sls_quantity,
               sls_price
          )
          SELECT
          sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- handle invalid data
               ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- datatype casting
          END AS sls_order_dt,
          CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
               ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
          END AS sls_ship_dt,
          CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
               ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
          END AS sls_due_dt,
          CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
          THEN sls_quantity * ABS(sls_price)
          ELSE sls_sales -- Recalculate sales id original value is missing or incorrect
          END AS sls_sales,
          sls_quantity,
          CASE WHEN sls_price IS NULL OR sls_price <=0 
               THEN sls_sales / NULLIF(sls_quantity,0)
          ELSE sls_price
          END AS sls_price -- derive price if original value is invalid
          FROM bronze.crm_sales_details
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';

          PRINT '-----------------------------------';
          PRINT 'Loading ERP Tables'
          PRINT '-----------------------------------';

          -- ============================================================================
          --Inserting Cleaned data from bronzze into Silver in silver.erp_cust_az12 table
          -- ============================================================================
          SET @start_time = GETDATE();
          PRINT '>> Truncating Table: silver.erp_cust_az12'
          TRUNCATE TABLE silver.erp_cust_az12;
          PRINT '>> Inserting Data Into: silver.erp_cust_az12'
          INSERT INTO silver.erp_cust_az12(
               cid,
               bdate,
               gen
          )
          SELECT
          CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
               ELSE cid
          END cid,
          CASE WHEN bdate > GETDATE() THEN NULL
               ELSE bdate
          END AS bdate,
          CASE 
               WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), ' ', '')) 
                    IN ('F', 'FEMALE') THEN 'Female'
               WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(gen)), CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), ' ', '')) 
                    IN ('M', 'MALE') THEN 'Male'
               ELSE 'n/a'
          END AS gen
          FROM bronze.erp_cust_az12
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';


          -- ============================================================================
          -- Inserting Cleaned data from bronzze into Silver in silver.erp_loc_a101 table
          -- ============================================================================
          
          SET @start_time = GETDATE();
          PRINT '>> Truncating Table: silver.erp_loc_a101'
          TRUNCATE TABLE silver.erp_loc_a101;
          PRINT '>> Inserting Data Into: silver.erp_loc_a101'
          INSERT INTO silver.erp_loc_a101(
          cid,
          cntry
          )
          SELECT
          REPLACE(cid, '-', '') AS cid, 
          CASE 
               WHEN clean = '' THEN 'n/a'
               WHEN clean = 'DE' THEN 'Germany'
               WHEN clean IN ('US','USA') THEN 'United States'
               ELSE cntry
          END AS cntry
          FROM (
          SELECT 
               cid,
               cntry,
               UPPER(
                    REPLACE(
                    REPLACE(
                    REPLACE(
                    REPLACE(
                    REPLACE(LTRIM(RTRIM(cntry)), CHAR(9), ''),   -- tab
                    CHAR(10), ''),                                 -- newline
                    CHAR(13), ''),                                 -- carriage return
                    CHAR(160), ''),                                -- non-breaking space
                    ' ', '')                                       -- normal space
               ) AS clean
          FROM bronze.erp_loc_a101
          ) AS cleaned;
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';



          -- ============================================================================
          -- Inserting Cleaned data from bronzze into Silver in silver.erp_px_cat_g1v2 table
          -- ============================================================================
          
          SET @start_time = GETDATE();
          PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
          TRUNCATE TABLE silver.erp_px_cat_g1v2;
          PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
          INSERT INTO silver.erp_px_cat_g1v2(
               id,
               cat,
               subcat,
               maintenance
          )
          SELECT
          id,
          cat,
          subcat,
          maintenance
          FROM bronze.erp_px_cat_g1v2
          SET @end_time = GETDATE();
          PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
          PRINT '>>---------------';

          SET @batch_end_time = GETDATE();
          PRINT '================================================'
          PRINT 'LOADING SILVER LAYER IS COMPLETED'
          PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time ) AS NVARCHAR) + ' seconds';
          PRINT '================================================'

     END TRY
     BEGIN CATCH
          PRINT '================================================'
          PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
          PRINT 'Error Message' + ERROR_MESSAGE();
          PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR );
          PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
          PRINT '================================================'
     END CATCH
END
 
EXEC silver.load_silver
