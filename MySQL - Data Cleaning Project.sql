-- MySQL - Data Cleaning

-- Step 1: Display all data from the "layoffs" table
SELECT * 
FROM layoffs;

-- Step 2: Create a staging table to keep raw data and avoid modifying the original table
CREATE TABLE layoffs_staging 
LIKE layoffs; -- Creates an empty table structure like "layoffs"
SELECT * 
FROM layoffs_staging;

-- Insert data into the staging table from the original table
INSERT INTO layoffs_staging 
SELECT * 
FROM layoffs;

-- Step 3: Remove Duplicates

-- Add row number to each row to identify duplicates
SELECT *, 
       ROW_NUMBER() OVER () AS row_num 
FROM layoffs_staging;

-- Find duplicates based on "company" column
SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY company) AS row_num -- Identifies duplicate company names
FROM layoffs_staging;

-- Filter records where company name starts with 'oy'
SELECT * 
FROM layoffs_staging
WHERE company LIKE 'oy%';

-- Find duplicates based on multiple columns (company, industry, total_laid_off, etc.)
SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num 
FROM layoffs_staging;

-- Create a Common Table Expression (CTE) to identify duplicates
WITH duplicate_cte AS (
  SELECT *, 
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
         ) AS row_num
  FROM layoffs_staging
)
-- Select duplicate rows based on the CTE
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Delete duplicate rows from the staging table using CTE
WITH duplicate_cte AS (
  SELECT *, 
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
         ) AS row_num
  FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- Step 4: Create a new table with cleaned data and remove duplicates
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert cleaned data into the new table
INSERT INTO layoffs_staging2
SELECT *, 
       ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num 
FROM layoffs_staging;

-- Delete duplicate rows from the new table
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 5: Standardize the Data

-- Remove leading and trailing spaces from the "company" column
SELECT company, 
       TRIM(company) AS trimmed_company
FROM layoffs_staging2;

-- Update the "company" column to remove spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Count distinct industries in the "layoffs_staging2" table
SELECT COUNT(DISTINCT industry) AS unique_industry_count
FROM layoffs_staging2;

-- Standardize industry names where "crypt%" appears
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypt%';

-- Remove unnecessary period from the "country" column
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United Stat%';

-- Convert the "date" column to a standard date format
SELECT `date`, 
       STR_TO_DATE(`date`, '%m/%d/%Y') AS default_sql_date_format_YYYY_MM_DD 
FROM layoffs_staging2;

-- Update "date" column to standard SQL date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter the column type of "date" to a proper DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 6: Handle Null or Blank Values

-- Find rows with null or blank values in relevant columns
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Find rows where "industry" is null or blank
SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Replace blank values with null in the "industry" column
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Update "industry" from a related table where "industry" is null or blank
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL OR t1.industry = ''
  AND t2.industry IS NOT NULL;

-- Step 7: Remove Unnecessary Columns

-- Delete rows with both "total_laid_off" and "percentage_laid_off" being null
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Drop the "row_num" column as it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final Check - Test with specific company (e.g., "Bally's")
SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'bally%';

-- Update "industry" for company "Bally's"
UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company LIKE 'bally%';
