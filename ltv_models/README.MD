Remember to run on virenv bearflowtest
login to gcloud
source ~/.profile
Why LTV = arpu / churn

Assume each period ARPU is same:
1. For the first stage,  the revenue is ARPU * (1- CHURN)
2. For the Nth stage, the revenue is ARPU * (1 - CHURN)^N
3. Sum all the revenue, it is ARPU *(1 - RETENTION^N) / CHURN.
4. Assume N is infinity, then all the revenue this user created is ARPU/CHURN

