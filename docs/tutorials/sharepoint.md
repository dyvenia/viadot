# How to pull excel file from Sharepoint 

With Viadot you can download Excel file from Sharepoint and then upload it to Azure Data Lake. You can set a URL to file on Sharepoint an specify parameters such as path to local Excel file, number of rows and sheet number to be extracted. 

## Pull data from Sharepoint and save output as a csv file on Azure Data Lake

To pull Excel file from Sharepint we create flow basing on `SharepointToADLS`
:::viadot.flows.SharepointToADLS 
