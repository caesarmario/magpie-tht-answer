### Magpie Technical Test Answer
##### by Mario Caesar // caesarmario87@gmail.com

# Study Case
Unexpected changes to the data structure, problems with data extraction, and website modifications can all cause discrepancies when using data from other sources, such as web scraping. Data for Q3 2024 in this case comprises the following attributes: 
- Product Name
- Date
- Total Historical Sales
- Daily Sales Total
In several goods, experts saw abnormal increases toward the end of Q3, with historical sales statistics rising to around ten times their usual values before declining once more. This README provides mitigation measures and a methodical approach to determining the underlying problems.

# Root Cause Analysis
Based on the case study above and as Data Engineer, below are potential causes of these spikes:
1. **HTML Structure Changes**: Unexpectedly layout or class name changes on websites might result in inaccurate values being collected by scrapers.
2. **Duplicate Records**: Duplicate scrapes may result from mistakes in Airflow DAG scheduling or retry procedures, particularly if scrapers are set up to retry in the event of failure.
3. **Pipeline bugs**: Accidental aggregations or mistakes in data processing scripts might cause previous sales figures to be inflated.
4. **Promotions, sales activity, and other external factors**: Sales data may see real increases as a result of marketing campaign, promotions, or other external activities.

# Mitigation that Can be Implemented
1. **Instant alerts and monitoring**: Use Slack, email, or any messaging tools to send real-time notifications monitoring in Airflow for any discrepancies found in daily sales totals. If there are data loading failures, use Airflow's retry rules sparingly to prevent repeated insertion.
2. **Fixing the data manually**: Work with data analysts to fix any impacted records in BigQuery that have irregularities found. Keep a backup of the updated data so that it may be reanalyzed if necessary. Verify that the original values were accurately recorded by looking at the raw scraped data that is kept in Google Cloud Storage (GCS). If the numbers are different, the problem most likely started when the material was being scrapped or loaded. To find failed or retried tasks that could have resulted in duplicate entries, examine the Airflow logs.
3. **Fallback mechanism**: To keep the analysis consistent if an anomaly is found, substitute the inaccurate record with the newly ingested data or with the average or median sales amount from the days that surround it.
4. **Implement data validation DAG**: Pre- and post-ingestion checks may be carried out by integrating data validation DAGs into Airflow. Plan on regularly validating historical data to identify any inconsistencies that may exist retrospectively. Custom Python validation scripts may be used to build jobs that compare daily sales data to a median or rolling average in order to spot notable variations. Set up notifications when sales numbers above a predetermined level, such as five times or more.
5. **Using 3rd party tools [link here](https://www.diffbot.com/)**: Use tools such as Diffbot or custom web monitors that alert you when page layouts change to keep an eye on the structure of your website. Automatically stop scraping DAGs until the structure is confirmed if it changes.

# Special Case Mitigation
1. **Managing End-of-Series vs Mid-Series Spikes**: We can use either forward/backward filling or imputation based on median sales figures for Mid-Series Spikes. When a time series has spikes in the center, this works well. However, for end-of-series, It's critical to look into recent scraping procedures or website modifications for end-of-period spikes because the data source could have changed close to the conclusion of the period. * Generated from ChatGPT *
2. **Limited data (3-4 days)**: Since automated anomaly detection algorithms may overfit or misunderstand spikes, human inspections are crucial for goods with relatively few data points. Before performing automated tests, think about establishing a minimum data day threshold.