USE portfolioproject;

-- Creating two empty databases with the same column lengths as the CSV files i intend to upload
CREATE TABLE coviddeaths (
iso_code VARCHAR(20),
continent VARCHAR(20),
location VARCHAR(50),
date_collected DATE,
population VARCHAR(20),
total_cases VARCHAR(20),
new_cases VARCHAR(20),
new_cases_smoothed VARCHAR(20),
total_deaths VARCHAR(20),
new_deaths VARCHAR(20),
new_deaths_smoothed VARCHAR(20),
total_cases_per_million VARCHAR(20),
new_cases_per_million VARCHAR(20),
new_cases_smoothed_per_million VARCHAR(20),
total_deaths_per_million VARCHAR(20),
new_deaths_per_million VARCHAR(20),
new_deaths_smoothed_per_million VARCHAR(20),
reproduction_rate VARCHAR(20),
icu_patients VARCHAR(20),
icu_patients_per_million VARCHAR(20),
hosp_patients VARCHAR(20),
hosp_patients_per_million VARCHAR(20),
weekly_icu_admissions VARCHAR(20),
weekly_icu_admissions_per_million VARCHAR(20),
weekly_hosp_admissions VARCHAR(20),
weekly_hosp_admissions_per_million VARCHAR(20));

CREATE TABLE covidvaccinations(
iso_code VARCHAR(20),
continent VARCHAR(20),
location VARCHAR(50),
date_collected DATE,
new_tests VARCHAR(20),
total_tests VARCHAR(20),
total_tests_per_thousand VARCHAR(20),
new_tests_per_thousand VARCHAR(20),
new_tests_smoothed VARCHAR(20),
new_tests_smoothed_per_thousand VARCHAR(20),
positive_rate VARCHAR(20),
test_per_case VARCHAR(20),
tests_units VARCHAR(20),
total_vaccinations VARCHAR(20),
people_vaccinated VARCHAR(20),
people_fully_vaccinated VARCHAR(20),
new_vaccinations VARCHAR(20),
new_vaccinations_smoothed VARCHAR(20),
total_vaccinations_per_hundred VARCHAR(20),
people_vaccinated_per_hundred VARCHAR(20),
people_fully_vaccinated_per_hundred VARCHAR(20),
new_vaccinations_smoothed_per_million VARCHAR(20),
stringency_index VARCHAR(20),
population_density VARCHAR(20),
median_age VARCHAR(20),
aged_65_older VARCHAR(20),
aged_70_older VARCHAR(20),
gdp_per_capita VARCHAR(20),
extreme_poverty VARCHAR(20),
cardiovasc_death_rate VARCHAR(20),
diabetes_prevalence VARCHAR(20),
female_smokers VARCHAR(20),
male_smokers VARCHAR(20),
handwashing_facilities VARCHAR(20),
hospital_beds_per_thousand VARCHAR(20),
life_expectancy VARCHAR(20),
human_development_index VARCHAR(20));

SHOW TABLES;
DESCRIBE coviddeaths;
DESCRIBE covidvaccinations;

-- Shows secure file path where my CSV files should be stored
SHOW VARIABLES LIKE 'secure_file_priv';

-- Loading data into the database with the data files stored in the file path identified above
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidDeaths.csv' 
INTO TABLE coviddeaths 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidVaccinations.csv' 
INTO TABLE covidvaccinations 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- Checking the number of rows in each database
SELECT COUNT(*) FROM coviddeaths;
SELECT COUNT(*) FROM covidvaccinations;

SELECT *
FROM coviddeaths
ORDER BY 3 , 4;

SELECT 
    location,
    date_collected,
    total_cases,
    new_cases,
    total_deaths,
    population
FROM
    coviddeaths
ORDER BY location , date_collected;

-- Looking at Total Cases vs Total Deaths in the Canada
-- Shows likelihood of dying if you contract covid in Canada
SELECT 
    location,
    date_collected,
    total_cases,
    total_deaths,
    (total_deaths / total_cases) * 100 AS death_percentage
FROM
    coviddeaths
WHERE
    location = 'Canada'
ORDER BY location , date_collected;

-- Looking at Total Cases vs Population in the Canada
-- Shows percentage of population that contracted covid19 in Canada
SELECT 
    location,
    date_collected,
    total_cases,
    population,
    (total_cases / population) * 100 AS infection_rate
FROM
    coviddeaths
WHERE
    location = 'Canada'
ORDER BY location , date_collected;

-- Countries with Highest Infection Rate compared to Population
SELECT
    location,
    population,
    MAX(total_cases) AS highest_infection_count,
    MAX(total_cases / population) * 100 AS percent_population_infected
FROM
    coviddeaths
GROUP BY location , population
ORDER BY percent_population_infected DESC;

-- Countries with Highest Death Count per Population
SELECT
    location,
    MAX(CAST(total_deaths AS UNSIGNED)) AS total_death_count
FROM
    coviddeaths
WHERE
    continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count DESC;


-- BREAKING THINGS DOWN BY CONTINENT
-- Showing contintents with the highest death count per population
SELECT 
    continent,
    MAX(CAST(total_deaths AS UNSIGNED)) AS total_death_count
FROM
    coviddeaths
WHERE
    continent IS NOT NULL
GROUP BY continent
ORDER BY total_death_count DESC;

-- GLOBAL NUMBERS
SELECT 
    SUM(new_cases) AS total_cases,
    SUM(new_deaths) AS total_deaths,
    (SUM(new_deaths) / SUM(new_cases)) * 100 AS death_percentage
FROM
    coviddeaths
WHERE
    continent IS NOT NULL
ORDER BY total_cases , total_deaths;

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine
Select 
	dea.continent, 
	dea.location, 
    dea.date_collected, 
    dea.population, 
    vac.new_vaccinations,
    SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.date_collected) as rolling_people_vaccinated, 
    (SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.date_collected)/dea.population)*100 as percentage_vaccinated
From coviddeaths dea
Join covidvaccinations vac
	On dea.location = vac.location
	and dea.date_collected = vac.date_collected
where dea.continent is not null 
order by dea.location,dea.date_collected;


-- Using CTE to perform Calculation on Partition By in previous query
WITH PopvsVac (continent, location, date_collected, population, new_vaccinations, rolling_people_vaccinated)
as
(
Select 
	dea.continent, 
	dea.location, 
    dea.date_collected, 
    dea.population, 
    vac.new_vaccinations,
    SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.date_collected) as rolling_people_vaccinated
From coviddeaths dea
Join covidvaccinations vac
	On dea.location = vac.location
	and dea.date_collected = vac.date_collected
where dea.continent is not null 
order by dea.location,dea.date_collected
)
Select *, (rolling_people_vaccinated/population)*100
From PopvsVac;

-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists PercentPopulationVaccinated;
CREATE TABLE PercentPopulationVaccinated (
    continent VARCHAR(50),
    location VARCHAR(50),
    date DATETIME,
    population VARCHAR(50),
    new_vaccinations VARCHAR(50),
    rolling_people_vaccinated NUMERIC
);

Insert into PercentPopulationVaccinated
Select 
	dea.continent, 
	dea.location, 
    dea.date_collected, 
    dea.population, 
    vac.new_vaccinations,
    SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.date_collected) as rolling_people_vaccinated
From coviddeaths dea
Join covidvaccinations vac
	On dea.location = vac.location
	and dea.date_collected = vac.date_collected;
    
SELECT 
    *, (rolling_people_vaccinated / population) * 100 as percentage_vaccinated
FROM
    PercentPopulationVaccinated;


-- Creating View to store data for later visualizations
Create View PercentPopulationVaccinated1 as
Select 
	dea.continent, 
	dea.location, 
    dea.date_collected, 
    dea.population, 
    vac.new_vaccinations,
    SUM(vac.new_vaccinations) OVER (Partition by dea.location Order by dea.location, dea.date_collected) as rolling_people_vaccinated
From coviddeaths dea
Join covidvaccinations vac
	On dea.location = vac.location
	and dea.date_collected = vac.date_collected
WHERE dea.continent is not null; 
