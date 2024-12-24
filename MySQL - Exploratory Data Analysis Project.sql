-- MySQL - Exploratory Data Analysis

-- Exploring the data in the 'layoffs_staging2' table

-- Select all records
SELECT *
FROM layoffs_staging2;

-- Finding the maximum layoffs and percentage of layoffs
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Filter specific layoffs data for Brazil and Germany
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
AND (country = 'Brazil' AND total_laid_off = 4) 
   OR (country = 'Germany' AND total_laid_off = 100);

-- List layoffs data ordered by funds raised (in millions)
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Total layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Finding the minimum and maximum dates of layoffs
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Total layoffs by industry (sector)
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Total layoffs by date
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC;

-- Total layoffs by year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

-- Total layoffs by month (without distinguishing the year)
SELECT MONTH(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY 2 DESC;

-- Order all records by funds raised (in millions)
SELECT *
FROM layoffs_staging2
ORDER BY funds_raised_millions DESC;

-- Extract month from the 'date' field
SELECT SUBSTRING(`date`, 6, 2) AS month
FROM layoffs_staging2;

-- Mapping month numbers to month names using CASE statement
SELECT 
    SUBSTRING(`date`, 6, 2) AS month_number,
    CASE SUBSTRING(`date`, 6, 2)
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        ELSE '---'
    END AS month_name
FROM layoffs_staging2;

-- Alternative method to map month numbers to month names using ELT function
SELECT 
    SUBSTRING(`date`, 6, 2) AS month_number,
    ELT(SUBSTRING(`date`, 6, 2), 
        'January', 'February', 'March', 'April', 'May', 'June', 
        'July', 'August', 'September', 'October', 'November', 'December') AS month_name
FROM layoffs_staging2;

-- Total layoffs by month, ordered by the sum of layoffs
SELECT 
    SUBSTRING(`date`, 6, 2) AS month_number,    
    ELT(SUBSTRING(`date`, 6, 2),
        'January', 'February', 'March', 'April', 'May', 'June', 
        'July', 'August', 'September', 'October', 'November', 'December') AS month_name,
    SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY month_number, month_name
ORDER BY total_laid_off_sum DESC;

-- Total layoffs by month and year
SELECT 
    SUBSTRING(`date`, 1, 7) AS month_number,    
    ELT(SUBSTRING(`date`, 6, 2),
        'January', 'February', 'March', 'April', 'May', 'June', 
        'July', 'August', 'September', 'October', 'November', 'December') AS month_name,
    SUBSTRING(`date`, 1, 4) AS `year`,
    SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY month_number, month_name, `year`
ORDER BY total_laid_off_sum DESC;

-- Concatenate month name and year for a formatted month-year string
SELECT 
    month_year_number, month_name,
    `year`,
    CONCAT(month_name, ' of ', `year`) AS month_year_name,
    SUM(total_laid_off) AS total_laid_off_sum
FROM (
    SELECT 
        SUBSTRING(`date`, 1, 7) AS month_year_number,    
        ELT(SUBSTRING(`date`, 6, 2),
            'January', 'February', 'March', 'April', 'May', 'June', 
            'July', 'August', 'September', 'October', 'November', 'December') AS month_name,
        SUBSTRING(`date`, 1, 4) AS `year`,
        total_laid_off
    FROM layoffs_staging2
) AS subquery
GROUP BY month_name, `year`, month_year_number
HAVING `year` IS NOT NULL
ORDER BY total_laid_off_sum DESC;

-- Calculating rolling total of layoffs by month
WITH rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `MONTH`
    ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;

-- Total layoffs by company (aggregating all branches)
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Total layoffs by company, year-wise
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

-- Total layoffs by company and year, ordered by the year with the most layoffs
SELECT company, YEAR(`date`), SUM(total_laid_off) AS total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total DESC;

-- Ranking companies by layoffs for each year
WITH company_year (company, years, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off) AS total
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
)
SELECT *,
    DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY ranking ASC;

-- Ranking top 5 companies with the most layoffs per year
WITH company_year (company, years, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off) AS total
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
company_year_rank AS (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5;
