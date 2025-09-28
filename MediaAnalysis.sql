Use rpc17;

# Request 1: Monthly Circulation Drop Check
WITH monthly_data AS (
    SELECT 
        c.city,
        Concat(year, '-', LPAD(month,2,'0')) as Month_YYYYMM,
        SUM(f.Net_Circulation) AS monthly_circulation
    FROM fact_print_sales f
    JOIN dim_city c
      ON f.City_ID = c.city_id
    GROUP BY c.city, f.year, f.month
),
with_lag AS (
    SELECT 
        city,
        Month_YYYYMM,
        monthly_circulation,
        LAG(monthly_circulation) OVER (PARTITION BY city ORDER BY Month_YYYYMM) AS prev_circulation
    FROM monthly_data
),
declines AS (
    SELECT 
        city,
        Month_YYYYMM,
        monthly_circulation,
        prev_circulation,
        (monthly_circulation - prev_circulation) AS mom_change
    FROM with_lag
    WHERE prev_circulation IS NOT NULL
      AND monthly_circulation < prev_circulation
)
SELECT 
    city AS city_name,
    Month_YYYYMM,
    monthly_circulation AS net_circulation,
    prev_circulation AS Prev_net_circulation,
    mom_change
FROM declines
ORDER BY mom_change ASC
LIMIT 3;

#Request 2 : Yearly Revenue Concentration by Category
Select 
	ad.year, 
    ad_category,
    ac.standard_ad_category as category_name, 
    Sum(ad.ad_revenue_INR) as category_revenue,
    Total_rev.Total_revenue_year,
    Round(Sum(ad.ad_revenue_INR)/Total_rev.Total_revenue_year *100,2) as pct_of_year_total
From fact_ad_revenue as ad
JOIN dim_ad_category as ac
	ON ad.ad_category = ac.ad_category_id
Join ( 	Select year,Sum(ad_revenue_INR) AS Total_revenue_year
		FROM fact_ad_revenue
		Where year Between 2019 and 2024
		Group by year
		Order by year) as Total_rev
	ON Total_rev.year = ad.year
Where ad.year between 2019 and 2024
Group by ad.year, ad.ad_category
Order by ad.year;

#Request 3: 2024 Print Efficient Leaderboard
With city_efficiency as (
	Select 
	dc.city as City,
    Sum(ps.copies_printed) as Copies_printed_2024,
    Sum(ps.Net_circulation) as Net_Circulation_2024,
    Round(Sum(ps.Net_Circulation)/Sum(ps.copies_printed) *100,4) as Efficiency_ratio_2024
From fact_print_sales ps
Join dim_city dc
	On ps.City_ID = dc.city_id
Where year = 2024
group by City
Order by Efficiency_ratio_2024 DESC)
Select * , 
	Rank() over (Order by Efficiency_ratio_2024 DESC)  as rnk
from city_efficiency 
Limit 5;

#Request 4: Internet Readiness Growth(2021)
with internet_rate_q1 as (
	Select city_id, internet_penetration 
    From fact_city_readiness
    Where year = 2021 and quarter = 'Q1'
),
internet_rate_q4 as (
	Select city_id, internet_penetration 
    From fact_city_readiness
    Where year = 2021 and quarter = 'Q4'
)
Select 
	city,
    q1.internet_penetration as internet_rate_Q1,
    q4.internet_penetration as internet_rate_Q4,
    Round(q4.internet_penetration - q1.internet_penetration,2) AS delta_internet_rate
from dim_city c
Join internet_rate_q1 as q1
	ON q1.city_id = c.city_id
Join internet_rate_q4 as q4
	ON q4.city_id = c.city_id
Order by delta_internet_rate DESC;

#Request 5: Consistent Multi-year Decline(2019-2024)
WITH yearly AS (
    SELECT 
        c.city,
        f.year,
        SUM(f.Net_Circulation) AS yearly_net_circulation,
        SUM(ad.ad_revenue_INR)   AS yearly_ad_revenue
    FROM fact_print_sales f
    JOIN dim_city c
      ON f.city_id = c.city_id
	JOIN fact_ad_revenue ad
		ON ad.edition_id = f.edition_ID
    WHERE f.year BETWEEN 2019 AND 2024
    GROUP BY c.city, f.year
)
, declines AS (
    SELECT 
        y.*,
        CASE 
          WHEN LAG(yearly_net_circulation) OVER (PARTITION BY city ORDER BY year) > yearly_net_circulation
          THEN 1 ELSE 0 END AS circulation_decline,
        CASE 
          WHEN LAG(yearly_ad_revenue) OVER (PARTITION BY city ORDER BY year) > yearly_ad_revenue
          THEN 1 ELSE 0 END AS revenue_decline
    FROM yearly y
)
, summary as(
    SELECT 
        city,
        MIN(year) AS start_year,
        MAX(year) AS end_year,
        SUM(circulation_decline) AS circulation_declines,
        SUM(revenue_decline) AS revenue_declines,
        COUNT(*) - 1 AS comparisons  -- 6 years → 5 comparisons
    FROM declines
    GROUP BY city
)
SELECT 
    d.city,
    d.year,
    d.yearly_net_circulation,
    d.yearly_ad_revenue,
    CASE WHEN s.circulation_declines = s.comparisons THEN 'Yes' ELSE 'No' END AS is_declining_print,
    CASE WHEN s.revenue_declines = s.comparisons THEN 'Yes' ELSE 'No' END AS is_declining_ad_revenue,
    CASE WHEN 
        s.circulation_declines = s.comparisons
        AND s.revenue_declines = s.comparisons
        THEN 'Yes' ELSE 'No' END AS is_declining_both
FROM declines d
JOIN summary s
  ON d.city = s.city
ORDER BY d.city, d.year; 

#Business request 6: 2021 readiness vs Pilot engagement outlier
WITH readiness AS (
    SELECT 
        c.city,
        Round(Sum(r.literacy_rate + r.smartphone_penetration + r.internet_penetration)/3,2) AS readiness_score_2021
    FROM fact_city_readiness r
    JOIN dim_city c
      ON r.city_id = c.city_id
    WHERE r.year = 2021
    GROUP BY c.city
),
engagement AS (
    SELECT 
        c.city,
        SUM(d.downloads_or_accesses) AS engagement_metric_2021
    FROM fact_digital_pilot d
    JOIN dim_city c
      ON d.city_id = c.city_id
    WHERE d.year = 2021
    GROUP BY c.city
),
combined AS (
    SELECT 
        re.city,
        re.readiness_score_2021,
        en.engagement_metric_2021,
        RANK() OVER (ORDER BY re.readiness_score_2021 DESC) AS readiness_rank_desc,
        RANK() OVER (ORDER BY en.engagement_metric_2021 ASC) AS engagement_rank_asc
    FROM readiness re
    JOIN engagement en
      ON re.city = en.city
)
SELECT 
    city,
    readiness_score_2021,
    engagement_metric_2021,
    readiness_rank_desc,
    engagement_rank_asc,
    CASE WHEN readiness_rank_desc = 1 AND engagement_rank_asc <= 3 THEN 'Yes' ELSE 'No' END AS is_outlier
FROM combined
ORDER BY city;


