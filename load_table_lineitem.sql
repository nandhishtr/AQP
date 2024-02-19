use skew_s100_z2;
BULK INSERT skew_s100_z2.dbo.lineitem FROM 'E:\TPCDSkew\lineitem100.tbl'
with (FORMAT = 'CSV', ROWTERMINATOR = '|\n', FIELDTERMINATOR = '|');
