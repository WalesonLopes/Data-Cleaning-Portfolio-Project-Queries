-- Project 1 - MySQL - Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns
# Alex sempre segue essa ordem

create table layoffs_staging
like layoffs; # copia a tabela, sem dados

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs; # adiciona os valores, é bom sempre manter a tabela original (raw data) em caso de algum erro e pra referência

-- 1. Remove Duplicates

select *,
row_number() over() as umalinhacada
from layoffs_staging
;

select *,
row_number() over(
partition by company) as empresa # pra achar empresas com nome repetido
from layoffs_staging
;

select *
from layoffs_staging
where company like 'oy%'
;

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num # mostra o que é duplicado
from layoffs_staging
;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num # todas colunas, listadas com column_name
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Casper'
;

SELECT column_name # lista o nome de todas as colunas pra poder copiar e não digitar uma por uma (não tem uma opção 'list all columns')
FROM information_schema.columns
WHERE table_name = 'layoffs_staging';

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num # todas colunas, listadas com column_name
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num # todas colunas, listadas com column_name
from layoffs_staging; # colocou todo o conteúdo e a nova coluna row_num na tabela nova

delete
from layoffs_staging2
where row_num > 1; # apaga todas linhas duplicadas

select *
from layoffs_staging2
where row_num > 1
;

select *
from layoffs_staging2
;

-- 2. Standardize the Data

select distinct(company)
from layoffs_staging2
;

select company, trim(company) AS trimmed_company # tira leading/trailing spaces
from layoffs_staging2
;

update layoffs_staging2
set company = trim(company) # atualiza a tabela e todos os nomes de empresas ficam sem espaços desnecessários
;

SELECT COUNT(DISTINCT industry) AS unique_industry_count
FROM layoffs_staging2;

select distinct industry
from layoffs_staging2
order by industry
;

select *
from layoffs_staging2
where industry like 'crypt%' # lista as linhas 'Crypto', 'Crypto Currency' e 'CryptoCurrency'
;

update layoffs_staging2
set industry = 'Crypto' # onde industry tem 'crypt%', transforma todas em 'Crypto' (padroniza os dados)
where industry like 'crypt%' 
;

select distinct country
from layoffs_staging2
order by country
;

select distinct country, trim(trailing '.' from country) # tira esse caractere do final
from layoffs_staging2
order by country
;

update layoffs_staging2
set country = trim(trailing '.' from country) # onde country tem 'United Stat%', tira o caractere '.' (padroniza os dados)
where country like 'United Stat%'
;

select `date`
from layoffs_staging2
;

select `date`,
str_to_date(`date`, '%m/%d/%Y') as default_sql_date_format_YYYY_MM_DD # Y maiúsculo é ano de 4 dígitos?
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y') # agora tá no formato de data padrão do MySQL, mas em formato text
;

alter table layoffs_staging2
modify column `date` date; # transforma em formato date

select *
from layoffs_staging2;

-- 3. Null Values or Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null # duas colunas com null, talvez sejam inúteis e seja bom remover
;

select *
from layoffs_staging2
where industry is null
or industry = '' # blank: ''
;

select *
from layoffs_staging2
where company = 'airbnb'
;

select *
from layoffs_staging2 table1
join layoffs_staging2 table2
	on table1.company = table2.company
where (table1.industry is null or table1.industry = '')
and table2.industry is not null
;

update layoffs_staging2
set industry = null
where industry = '' # transforma todos os blanks em nulls
;

update layoffs_staging2 table1
join layoffs_staging2 table2
	on table1.company = table2.company
set table1.industry = table2.industry
where table1.industry is null or table1.industry = '' # com base em uma condição, adiciona valores onde há valores nulos ou vazios
and table2.industry is not null
;

-- 4. Remove Any Columns

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null # apagou os dados de tudo que tinha essas duas colunas vazias
;

alter table layoffs_staging2
drop column row_num; # exclui só essa coluna

select *
from layoffs_staging2;

-- Teste próprio: colocar industry pra empresa Bally's (estava vazio)

select *
from layoffs_staging2
where company like 'bally%'
;

select distinct industry
from layoffs_staging2
order by 1
;

update layoffs_staging2
set industry = 'Travel'
where company like 'bally%'
;