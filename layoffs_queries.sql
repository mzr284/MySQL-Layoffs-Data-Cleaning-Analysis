SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs; 

SELECT *
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off
, percentage_laid_off, `date`, funds_raised_millions, industry) row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off
, percentage_laid_off, `date`, funds_raised_millions, industry) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

UPDATE layoffs_staging2
SET company = TRIM(company);

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE "United%";

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS  NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS  NULL OR t1.industry = '')
AND t2.industry != "";

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT,
  `total_stuff` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

INSERT INTO layoffs_staging3
SELECT *,
(total_laid_off / percentage_laid_off)
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL AND percentage_laid_off != ''
AND percentage_laid_off != 0;

SELECT *
FROM layoffs_staging3;

-- Exploratory Data

SELECT *
FROM layoffs_staging2;

SELECT company, AVG(total_laid_off) avg_layoff
FROM layoffs_staging2
GROUP BY company
ORDER BY avg_layoff DESC;

SELECT SUM(total_laid_off) sum_layoff
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL;

SELECT YEAR(`date`), SUM(total_laid_off) sum_layoff
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY sum_layoff DESC;

SELECT SUBSTRING(`date`, 6, 2) `month`, SUM(total_laid_off) sum_layoffs
FROM layoffs_staging2
GROUP BY `month`;

WITH CTE AS
(
SELECT SUBSTRING(`date`, 1, 7) `month`, SUM(total_laid_off) sum_layoffs
FROM layoffs_staging2
GROUP BY `month`
)
SELECT `month`, 
SUM(sum_layoffs) OVER(ORDER BY `month`) roll_total_layoffs
FROM CTE
WHERE `month` IS NOT NULL;

WITH CTE2 AS
(
SELECT company, YEAR(`date`) `year`, SUM(total_laid_off) sum_layoffs
FROM layoffs_staging2
GROUP BY company, `year`
ORDER BY company, `year`
), company_rank AS
(
SELECT company, `year`, sum_layoffs,
DENSE_RANK() OVER(PARTITION BY `year` ORDER BY sum_layoffs DESC) AS ranking,
(
SELECT AVG(sum_layoffs)
FROM CTE2
) year_avg_sum_layoffs
FROM CTE2
WHERE sum_layoffs IS NOT NULL AND `year` IS NOT NULL
)
SELECT *
FROM company_rank
WHERE ranking <= 5;

SELECT DISTINCT country
FROM layoffs_staging2;