-- Create Tables and import data into PostgreSQL
CREATE TABLE covid_deaths(
iso_code VARCHAR(50),
continent VARCHAR(100),
location VARCHAR(100),
date DATE,
population BIGINT,
total_cases BIGINT,
new_cases BIGINT,
new_cases_smoothed DECIMAL,
total_deaths BIGINT,
new_deaths BIGINT,
new_deaths_smoothed DECIMAL,
total_cases_per_million DECIMAL,
new_cases_per_million DECIMAL,
new_cases_smoothed_per_million DECIMAL,
total_deaths_per_million DECIMAL,
new_deaths_per_million DECIMAL,
new_deaths_smoothed_per_million DECIMAL,
reproduction_rate DECIMAL,
icu_patients DECIMAL,
icu_patients_per_million DECIMAL,
hosp_patients DECIMAL,
hosp_patients_per_million DECIMAL,
weekly_icu_admissions DECIMAL,
weekly_icu_admissions_per_million DECIMAL,
weekly_hosp_admissions DECIMAL,
weekly_hosp_admissions_per_million DECIMAL);

ALTER TABLE covid_deaths
ALTER COLUMN location TYPE VARCHAR(500);

CREATE TABLE covid_vaccinations(
iso_code VARCHAR(500),
continent VARCHAR(500),
location VARCHAR(500),
date DATE,
new_tests INTEGER,
total_tests INTEGER,
total_tests_per_thousand DECIMAL,
new_tests_per_thousand DECIMAL,
new_tests_smoothed INTEGER,
new_tests_smoothed_per_thousand DECIMAL,
positive_rate DECIMAL,
tests_per_case DECIMAL,
tests_units VARCHAR(500),
total_vaccinations INTEGER,
people_vaccinated INTEGER,
people_fully_vaccinated INTEGER,
new_vaccinations INTEGER,
new_vaccinations_smoothed INTEGER,
total_vaccinations_per_hundred DECIMAL,
people_vaccinated_per_hundred DECIMAL,
people_fully_vaccinated_per_hundred DECIMAL,
new_vaccinations_smoothed_per_million INTEGER,
stringency_index DECIMAL,
population_density DECIMAL,
median_age DECIMAL,
aged_65_older DECIMAL,
aged_70_older DECIMAL,
gdp_per_capita DECIMAL,
extreme_poverty DECIMAL,
cardiovascular_death_rate DECIMAL,
diabetes_prevalence DECIMAL,
female_smokers DECIMAL,
male_smokers DECIMAL,
handwashing_facilities DECIMAL,
hospital_beds_per_thousand DECIMAL,
life_expectancy DECIMAL,
human_development_index DECIMAL);

SELECT * FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY location,date;

--SELECT * FROM covid_vaccinations
--ORDER BY location,date;

--Select Data that we are going to be using
SELECT location,date,total_cases,new_cases,total_deaths,population
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY location,date;

--Looking at Total Cases vs. Total Deaths
--Shows the likelihood of dying if you contract COVID in your country
SELECT location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 AS death_percentage
FROM covid_deaths
WHERE location LIKE 'United States'
AND continent IS NOT NULL
ORDER BY location,date;

--Looking at Total Cases vs. Population
--Shows Percentage of Population that has contracted COVID by date in your country
SELECT location,date,population,total_cases,(total_cases/population)*100 AS infection_percentage
FROM covid_deaths
WHERE location LIKE 'United States'
AND continent IS NOT NULL
ORDER BY location,date;

--Looking at Countries with the Highest Infection Rate compared to Population
SELECT location,population,MAX(total_cases) as highest_infection_count,MAX((total_cases/population))*100 AS infection_percentage
FROM covid_deaths
GROUP BY location,population
ORDER BY infection_percentage DESC NULLS LAST;

--Showing the Countries with the Highest Death Count per Population
SELECT location, MAX(cast(total_deaths as int)) as total_death_count
FROM covid_deaths
WHERE continent is NOT NULL
GROUP BY location
ORDER BY total_death_count DESC NULLS LAST;

--Break things down by CONTINENT
--Showing the Continents with the Highest Death Count per Population
SELECT continent,MAX(cast(total_deaths as int)) as total_death_count
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY total_death_count DESC NULLS LAST;

--Global Numbers by Date
SELECT date,SUM(new_cases) as total_cases,SUM(new_deaths) as total_deaths,SUM(new_deaths)/SUM(new_cases)*100 AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY date;

--Total Global Numbers
SELECT SUM(new_cases) as total_cases,SUM(new_deaths) as total_deaths,SUM(new_deaths)/SUM(new_cases)*100 AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1,2;

--Looking at Total Population vs. Vaccinations
SELECT dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations
,SUM(vac.new_vaccinations) OVER (Partition by dea.location 
		  ORDER BY dea.location,dea.date) AS rolling_people_vaccinated
--,(rolling_people_vaccinated/population)*100
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY location,date;

--USE CTE to find Percentage of Population Vaccinated per Country
With PopvsVac (Continent,Location,Date,Population,new_vaccinations,rolling_people_vaccinated)
AS
(
SELECT dea.continent,dea.location,dea.date,CAST(dea.population AS numeric),vac.new_vaccinations
,SUM(vac.new_vaccinations) OVER(Partition by dea.location 
		  ORDER BY dea.location,dea.date) AS rolling_people_vaccinated
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
)
SELECT *,(rolling_people_vaccinated/population)*100 AS rolling_percentage 
FROM PopvsVac;

--Max Vaccination Percentage per Country using CTE
With PopvsMaxVac (Location,Population,max_people_vaccinated)
AS
(
SELECT dea.location,CAST(dea.population AS numeric)
,MAX(vac.total_vaccinations) AS max_people_vaccinated
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
WHERE dea.continent IS NOT NULL
GROUP BY dea.location,population
)
SELECT *,CAST((max_people_vaccinated/population)*100 AS decimal) AS max_vaccination_percentage
FROM PopvsMaxVac
ORDER BY max_vaccination_percentage DESC NULLS LAST;

--TEMP TABLE
DROP TABLE IF EXISTS percent_population_vaccinated;
SELECT dea.continent,dea.location,dea.date,CAST(dea.population AS numeric),vac.new_vaccinations
,SUM(vac.new_vaccinations) OVER(Partition by dea.location 
		  ORDER BY dea.location,dea.date) AS rolling_people_vaccinated
INTO TEMPORARY TABLE percent_population_vaccinated
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY location,date;

SELECT *,(rolling_people_vaccinated/population)*100 AS rolling_percentage
FROM percent_population_vaccinated;

--Creating Views to store data for later visualizations

CREATE VIEW percent_population_vaccinated AS
SELECT dea.continent,dea.location,dea.date,CAST(dea.population AS numeric),vac.new_vaccinations
,SUM(vac.new_vaccinations) OVER(PARTITION BY dea.location 
		  ORDER BY dea.location,dea.date) AS rolling_people_vaccinated
--,(rolling_people_vaccinated/population)*100
FROM covid_deaths AS dea
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL;

SELECT * 
FROM percent_population_vaccinated;

CREATE VIEW total_global_numbers AS
SELECT SUM(new_cases) as total_cases,SUM(new_deaths) as total_deaths,SUM(new_deaths)/SUM(new_cases)*100 AS death_percentage
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1,2;

SELECT *
FROM total_global_numbers;