# Avertra Data Engineer Assignment V1

### Description 
Water utilities customers prefer prior plan their water bill by knowing the subsequent water consumption, as a feature utilities provide their customers, they can pick the next consumption period to receive the forecasted amount of water usage in the dashboard way of visualization. AI team provides a time-series solution based on the univariate ARIMA method for a multi-models AI case, by training forecasting model for each client (tagged by OID), model per client. The water utility data is coming in the following shape:  
Daily updated, two attributes (date, and consumption) are feeding the database monthly by appending new OIDS and consumption.  
Building a Multi-models solution for this case and serving it based ML toolkit for Kubernetes performs good, but keep it in shape that the concern, in term:  
 - Time in fetching data.  
 - Time in retraining model.  
 - Time in evaluate model fed by new consumption data.   
 - We are looking for a solution to automate extracting data from the feed and use it to update the ML model with relative performance metrics to put it up for use/reuse.    
[Note: use your preferred Messaging platfrom and Scheduling platfrom to set up the environment]  
We need a high-level Arch for the environment.  
(Streaming simulation)  
Data is hosted in SQL server, for 27 k clients. we recommend mimicking streaming data by taking 20% as original data and the rest as streaming.   

### File
The AI forecasting water consumption solution based Arima univariate ML method. using Jupyter IDE.  
arima-univarient-solution.ipynb 
### Data Sample 
	ConsumptionDate	AccountOID	Consumption
1	2022-04-01	3216049	0.0  
2	2022-04-01	3216050	0.0  
3	2022-04-01	3097332	1.127  
4	2022-04-01	3120772	0.348  
5	2022-04-01	3173639	0.409  
6	2022-04-01	3230385	0.954  
7	2022-04-01	3217487	0.741  
8	2022-04-01	3230386	0.266  
9	2022-04-01	3067428	0.515  
10	2022-04-01	3254983	0.303  
11	2022-04-01	3254985	0.578  
12	2022-04-01	3280547	0.558  
13	2022-04-01	3230389	0.523  
### Recommended Platform for ETL
Kafka.   
AirFlow.   
MLFlow.   
### Database Connection

to be included ......   
     
driver ODBC Driver 17 for SQL Server   
server='******'    
database='******'   
uid='******'   
pwd='******'  
