{% docs pos_unified_funnel_ltv_ttest %}
# sdp-prd-commercial.mart.pos_unified_funnel_ltv_ttest

This model creates a t-test metric to be used in ExP experiment monitoring estimated LTV of MQLs and self-serve retail trial leads on specific /POS pages. 

A user converts if they: 
* Submit a Retail MQL 
* *OR* Successfully signs up for a retail free trial

Estimated LTV per conversion are constants. 
* Avg LTV per MQL	$9,996
* Avg LTV per SS lead	$108

## Caveats

This metric is created specifically with the Retail Unified Funnel experiment in mind (https://experiments.shopify.com/experiments/retail_unified_funnel). 

## Details

* **Grain**: A row represents a single subject within a given `experiment_handle`
* **Grain unique column set**: ['experiment_handle', 'subject_id']

## Playbook

```sql
-- Get total estimated LTV by experiment variant.
SELECT experiment_handle
      ,variant
      ,SUM(value) AS total_est_ltv
FROM sdp-prd-commercial.mart.pos_unified_funnel_ltv_ttest
GROUP BY experiment_handle
        ,variant
ORDER BY experiment_handle
        ,variant
```

{% enddocs %}
